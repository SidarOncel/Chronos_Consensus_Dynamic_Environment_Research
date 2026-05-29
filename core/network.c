#include "network.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * This file is the transport layer for the paper repo.
 *
 * It is responsible for:
 *   - opening UDP sockets,
 *   - sending packets to peers,
 *   - polling for incoming packets,
 *   - dispatching packets to callbacks,
 *   - maintaining a small peer table,
 *   - tracking communication statistics.
 *
 * The important idea is separation of concerns:
 *   - raft.c decides what the packet means,
 *   - network.c decides how the packet moves,
 *   - hal.c provides logging and timing.
 *
 * That separation already exists in full project, where the
 * communication layer carries RAFT messages and the RAFT layer only
 * focuses on consensus behavior. 
 */

/* -------------------------------------------------------------------------
 * Platform-specific socket support
 * -------------------------------------------------------------------------
 *
 * We want the same source file to compile on:
 *   - Linux
 *   - Windows
 *   - Milk-V Linux userspace
 *
 * The socket code differs slightly across platforms, so we hide those
 * details here.
 */
#ifdef _WIN32

    #define WIN32_LEAN_AND_MEAN
    #include <winsock2.h>
    #include <ws2tcpip.h>

    typedef SOCKET socket_t;
    #define SOCKET_INVALID INVALID_SOCKET
    #define CLOSE_SOCKET closesocket

#else

    #include <unistd.h>
    #include <fcntl.h>
    #include <arpa/inet.h>
    #include <sys/socket.h>
    #include <netinet/in.h>

    typedef int socket_t;
    #define SOCKET_INVALID (-1)
    #define CLOSE_SOCKET close

#endif

/* -------------------------------------------------------------------------
 * Internal protocol constants
 * -------------------------------------------------------------------------
 */

/*
 * A small magic value helps us reject garbage packets quickly.
 * Think of it like a tiny label that says:
 *   "this packet belongs to CHRONOS, not random noise"
 */
static const uint8_t PROTOCOL_MAGIC[4] = { 'C', 'H', 'R', 'N' };

#define PROTOCOL_VERSION 1
#define MAX_PEERS        16
#define MAX_CALLBACKS    16

/* -------------------------------------------------------------------------
 * Peer table entry
 * -------------------------------------------------------------------------
 *
 * Each peer represents one node in the cluster.
 *
 * Example:
 *   node 2 -> 127.0.0.1:8002
 */
typedef struct {
    uint8_t  node_id;
    char     ip_addr[16];
    uint16_t port;
    int8_t   rssi;
    uint64_t last_seen;
    bool     active;
} PeerEntry_t;

/* -------------------------------------------------------------------------
 * Callback table entry
 * -------------------------------------------------------------------------
 *
 * A callback is the function we run when a certain message type arrives.
 * This is how the network layer stays generic.
 */
typedef struct {
    MessageType_t     type;
    MessageCallback_t callback;
    bool              active;
} CallbackEntry_t;

/* -------------------------------------------------------------------------
 * Global communication state
 * -------------------------------------------------------------------------
 *
 * The whole module keeps its state in one place.
 * That makes it easier to debug and easier to summarize in the paper.
 */
static struct {
    socket_t socket_fd;
    uint8_t node_id;
    char local_ip[16];
    uint16_t port;

    PeerEntry_t peers[MAX_PEERS];
    CallbackEntry_t callbacks[MAX_CALLBACKS];

    uint32_t sequence_number;
    bool initialized;

    CommStatistics_t stats;
    uint8_t recv_buffer[COMM_MAX_PACKET_SIZE];
} g_comm;

/*
 * This override lets the application choose a custom port before init.
 * It is useful when running several nodes on the same machine.
 */
uint16_t g_comm_port_override = 0;

/* -------------------------------------------------------------------------
 * Internal helper declarations
 * -------------------------------------------------------------------------
 */
static ChronosResult_t socket_init(void);
static void socket_cleanup(void);
static bool socket_set_nonblocking(socket_t sock);
static PeerEntry_t* find_peer(uint8_t node_id);
static void process_received_message(const void* data, size_t len);

/* -------------------------------------------------------------------------
 * Initialization / shutdown
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_init(uint8_t node_id, const char* ip_addr)
{
    if (g_comm.initialized) {
        return CHRONOS_OK;
    }

#ifdef _WIN32
    /*
     * Windows requires socket startup before any networking calls.
     */
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        return CHRONOS_ERR_COMM_FAIL;
    }
#endif

    memset(&g_comm, 0, sizeof(g_comm));

    g_comm.node_id = node_id;

    /*
     * If no IP is provided, use localhost.
     * That is enough for the first paper experiments.
     */
    strncpy(g_comm.local_ip,
            ip_addr ? ip_addr : "127.0.0.1",
            sizeof(g_comm.local_ip) - 1);

    /*
     * The port can be overridden so multiple nodes can run on one host.
     */
    g_comm.port = (g_comm_port_override != 0)
                    ? g_comm_port_override
                    : COMM_RAFT_PORT;

    if (socket_init() != CHRONOS_OK) {
        return CHRONOS_ERR_COMM_FAIL;
    }

    g_comm.initialized = true;

    hal_console_log("[NET] initialized node=%u ip=%s port=%u",
                    g_comm.node_id,
                    g_comm.local_ip,
                    g_comm.port);

    return CHRONOS_OK;
}

ChronosResult_t comm_shutdown(void)
{
    if (!g_comm.initialized) {
        return CHRONOS_OK;
    }

    socket_cleanup();

#ifdef _WIN32
    WSACleanup();
#endif

    g_comm.initialized = false;

    hal_console_log("[NET] shutdown");
    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Socket setup
 * -------------------------------------------------------------------------
 */

static ChronosResult_t socket_init(void)
{
    /*
     * UDP is a good fit for consensus experiments because it keeps message
     * delivery visible and controllable.
     *
     * TCP would hide some of the network behavior we want to observe.
     */
    g_comm.socket_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (g_comm.socket_fd == SOCKET_INVALID) {
        return CHRONOS_ERR_COMM_FAIL;
    }

    if (!socket_set_nonblocking(g_comm.socket_fd)) {
        CLOSE_SOCKET(g_comm.socket_fd);
        g_comm.socket_fd = SOCKET_INVALID;
        return CHRONOS_ERR_COMM_FAIL;
    }

    int opt = 1;
    setsockopt(g_comm.socket_fd,
               SOL_SOCKET,
               SO_REUSEADDR,
               (const char*)&opt,
               sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_comm.port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(g_comm.socket_fd,
             (struct sockaddr*)&addr,
             sizeof(addr)) < 0) {
        CLOSE_SOCKET(g_comm.socket_fd);
        g_comm.socket_fd = SOCKET_INVALID;
        return CHRONOS_ERR_COMM_FAIL;
    }

    return CHRONOS_OK;
}

static void socket_cleanup(void)
{
    if (g_comm.socket_fd != SOCKET_INVALID) {
        CLOSE_SOCKET(g_comm.socket_fd);
        g_comm.socket_fd = SOCKET_INVALID;
    }
}

static bool socket_set_nonblocking(socket_t sock)
{
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(sock, FIONBIO, &mode) == 0;
#else
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(sock, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

/* -------------------------------------------------------------------------
 * Peer management
 * -------------------------------------------------------------------------
 */

static PeerEntry_t* find_peer(uint8_t node_id)
{
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_comm.peers[i].active &&
            g_comm.peers[i].node_id == node_id) {
            return &g_comm.peers[i];
        }
    }
    return NULL;
}

ChronosResult_t comm_register_peer(uint8_t node_id,
                                   const char* ip_addr,
                                   uint16_t port)
{
    if (!ip_addr || node_id == 0) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    /*
     * If the peer already exists, update it instead of adding a duplicate.
     */
    PeerEntry_t* existing = find_peer(node_id);
    if (existing) {
        strncpy(existing->ip_addr, ip_addr, sizeof(existing->ip_addr) - 1);
        existing->port = port;
        existing->last_seen = hal_get_time_ms();
        return CHRONOS_OK;
    }

    for (int i = 0; i < MAX_PEERS; i++) {
        if (!g_comm.peers[i].active) {
            g_comm.peers[i].active = true;
            g_comm.peers[i].node_id = node_id;
            strncpy(g_comm.peers[i].ip_addr, ip_addr, sizeof(g_comm.peers[i].ip_addr) - 1);
            g_comm.peers[i].port = port;
            g_comm.peers[i].rssi = 0;
            g_comm.peers[i].last_seen = hal_get_time_ms();

            hal_console_log("[NET] peer added node=%u %s:%u",
                            node_id, ip_addr, port);
            return CHRONOS_OK;
        }
    }

    return CHRONOS_ERR_NO_MEMORY;
}

ChronosResult_t comm_unregister_peer(uint8_t node_id)
{
    PeerEntry_t* peer = find_peer(node_id);
    if (!peer) {
        return CHRONOS_ERR_NOT_FOUND;
    }

    peer->active = false;
    return CHRONOS_OK;
}

ChronosResult_t comm_get_peer_info(uint8_t node_id, NodeInfo_t* info)
{
    if (!info) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    PeerEntry_t* peer = find_peer(node_id);
    if (!peer) {
        return CHRONOS_ERR_NOT_FOUND;
    }

    memset(info, 0, sizeof(*info));
    info->node_id = peer->node_id;
    inet_pton(AF_INET, peer->ip_addr, &info->ip_addr);
    info->port = peer->port;
    info->rssi = peer->rssi;
    info->last_seen = peer->last_seen;
    info->is_alive = peer->active ? 1 : 0;

    return CHRONOS_OK;
}

void comm_update_peer_rssi(uint8_t node_id, int8_t rssi)
{
    PeerEntry_t* peer = find_peer(node_id);
    if (peer) {
        peer->rssi = rssi;
    }
}

/* -------------------------------------------------------------------------
 * CRC32
 * -------------------------------------------------------------------------
 *
 * CRC is used to detect accidental packet corruption.
 * If a packet is damaged, the receiver can reject it early.
 */
uint32_t comm_calculate_crc32(const void* data, size_t len)
{
    if (!data || len == 0) {
        return 0;
    }

    const uint8_t* bytes = (const uint8_t*)data;
    uint32_t crc = 0xFFFFFFFFu;

    for (size_t i = 0; i < len; i++) {
        crc ^= bytes[i];

        for (int bit = 0; bit < 8; bit++) {
            if (crc & 1u) {
                crc = (crc >> 1) ^ 0xEDB88320u;
            } else {
                crc >>= 1;
            }
        }
    }

    return ~crc;
}

bool comm_verify_crc(const MessageHeader_t* header, const void* payload)
{
    if (!header) {
        return false;
    }

    uint8_t buffer[COMM_MAX_PACKET_SIZE];
    size_t total_len = sizeof(MessageHeader_t) + header->payload_len;

    if (total_len > sizeof(buffer)) {
        return false;
    }

    /*
     * Rebuild the packet in a local buffer, zero the CRC field, and compute
     * the checksum again.
     */
    memcpy(buffer, header, sizeof(MessageHeader_t));

    MessageHeader_t* temp = (MessageHeader_t*)buffer;
    uint32_t saved_crc = temp->crc32;
    temp->crc32 = 0;

    if (payload && header->payload_len > 0) {
        memcpy(buffer + sizeof(MessageHeader_t),
               payload,
               header->payload_len);
    }

    uint32_t calc_crc = comm_calculate_crc32(buffer, total_len);
    return calc_crc == saved_crc;
}

/* -------------------------------------------------------------------------
 * Callback registration
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_register_callback(MessageType_t type,
                                       MessageCallback_t callback)
{
    if (!callback) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    for (int i = 0; i < MAX_CALLBACKS; i++) {
        if (!g_comm.callbacks[i].active) {
            g_comm.callbacks[i].active = true;
            g_comm.callbacks[i].type = type;
            g_comm.callbacks[i].callback = callback;
            return CHRONOS_OK;
        }
    }

    return CHRONOS_ERR_NO_MEMORY;
}

ChronosResult_t comm_unregister_callback(MessageType_t type)
{
    for (int i = 0; i < MAX_CALLBACKS; i++) {
        if (g_comm.callbacks[i].active &&
            g_comm.callbacks[i].type == type) {
            g_comm.callbacks[i].active = false;
            g_comm.callbacks[i].callback = NULL;
            return CHRONOS_OK;
        }
    }

    return CHRONOS_ERR_NOT_FOUND;
}

/* -------------------------------------------------------------------------
 * Message sending
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_send_to_node(uint8_t dst_id,
                                  MessageType_t type,
                                  const void* payload,
                                  size_t payload_len)
{
    PeerEntry_t* peer = find_peer(dst_id);
    if (!peer) {
        return CHRONOS_ERR_NOT_FOUND;
    }

    if (payload_len + sizeof(MessageHeader_t) > COMM_MAX_PACKET_SIZE) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    uint8_t buffer[COMM_MAX_PACKET_SIZE];
    memset(buffer, 0, sizeof(buffer));

    MessageHeader_t* header = (MessageHeader_t*)buffer;

    memcpy(header->magic, PROTOCOL_MAGIC, sizeof(PROTOCOL_MAGIC));
    header->version = PROTOCOL_VERSION;
    header->term = 0;
    header->src_id = g_comm.node_id;
    header->dst_id = dst_id;
    header->type = (uint16_t)type;
    header->seq_num = g_comm.sequence_number++;
    header->payload_len = (uint32_t)payload_len;
    header->crc32 = 0;

    if (payload && payload_len > 0) {
        memcpy(buffer + sizeof(MessageHeader_t),
               payload,
               payload_len);
    }

    header->crc32 = comm_calculate_crc32(buffer,
                                         sizeof(MessageHeader_t) + payload_len);

    struct sockaddr_in dest;
    memset(&dest, 0, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(peer->port);
    inet_pton(AF_INET, peer->ip_addr, &dest.sin_addr);

    int sent = sendto(g_comm.socket_fd,
                      (const char*)buffer,
                      (int)(sizeof(MessageHeader_t) + payload_len),
                      0,
                      (struct sockaddr*)&dest,
                      sizeof(dest));

    if (sent < 0) {
        g_comm.stats.send_errors++;
        return CHRONOS_ERR_COMM_FAIL;
    }

    g_comm.stats.packets_sent++;
    g_comm.stats.bytes_sent += (uint64_t)sent;

    return CHRONOS_OK;
}

ChronosResult_t comm_broadcast(MessageType_t type,
                               const void* payload,
                               size_t payload_len)
{
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!g_comm.peers[i].active) {
            continue;
        }

        if (g_comm.peers[i].node_id == g_comm.node_id) {
            continue;
        }

        comm_send_to_node(g_comm.peers[i].node_id,
                          type,
                          payload,
                          payload_len);
    }

    return CHRONOS_OK;
}

ChronosResult_t comm_send_raw_udp(const char* ip_addr,
                                  uint16_t port,
                                  const void* data,
                                  size_t len)
{
    if (!ip_addr || !data || len == 0) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    struct sockaddr_in dest;
    memset(&dest, 0, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);

    if (inet_pton(AF_INET, ip_addr, &dest.sin_addr) <= 0) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    int sent = sendto(g_comm.socket_fd,
                      (const char*)data,
                      (int)len,
                      0,
                      (struct sockaddr*)&dest,
                      sizeof(dest));

    if (sent < 0) {
        g_comm.stats.send_errors++;
        return CHRONOS_ERR_COMM_FAIL;
    }

    g_comm.stats.packets_sent++;
    g_comm.stats.bytes_sent += (uint64_t)sent;

    return CHRONOS_OK;
}

ChronosResult_t comm_send_message(const MessageHeader_t* header,
                                  const void* payload)
{
    if (!header) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    return comm_send_to_node(header->dst_id,
                             (MessageType_t)header->type,
                             payload,
                             header->payload_len);
}

/* -------------------------------------------------------------------------
 * Message reception
 * -------------------------------------------------------------------------
 */

static void process_received_message(const void* data, size_t len)
{
    if (len < sizeof(MessageHeader_t)) {
        g_comm.stats.receive_errors++;
        return;
    }

    const MessageHeader_t* header = (const MessageHeader_t*)data;

    if (memcmp(header->magic, PROTOCOL_MAGIC, sizeof(PROTOCOL_MAGIC)) != 0) {
        g_comm.stats.receive_errors++;
        return;
    }

    const void* payload = (const uint8_t*)data + sizeof(MessageHeader_t);
    size_t payload_len = len - sizeof(MessageHeader_t);

    if (!comm_verify_crc(header, payload)) {
        g_comm.stats.crc_errors++;
        return;
    }

    /*
     * Ignore packets we sent ourselves.
     */
    if (header->src_id == g_comm.node_id) {
        return;
    }

    PeerEntry_t* peer = find_peer(header->src_id);
    if (peer) {
        peer->last_seen = hal_get_time_ms();
    }

    /*
     * Dispatch to the first matching callback.
     * This keeps the network layer generic and RAFT-specific behavior
     * inside raft.c.
     */
    for (int i = 0; i < MAX_CALLBACKS; i++) {
        if (!g_comm.callbacks[i].active) {
            continue;
        }

        if (g_comm.callbacks[i].type != (MessageType_t)header->type) {
            continue;
        }

        g_comm.callbacks[i].callback(header, payload, payload_len);
        break;
    }
}

uint32_t comm_poll(uint32_t timeout_ms)
{
    (void)timeout_ms;

    if (!g_comm.initialized) {
        return 0;
    }

    uint32_t processed = 0;

    while (1) {
        struct sockaddr_in from;
        socklen_t from_len = sizeof(from);

        int recv_len = recvfrom(g_comm.socket_fd,
                                (char*)g_comm.recv_buffer,
                                sizeof(g_comm.recv_buffer),
                                0,
                                (struct sockaddr*)&from,
                                &from_len);

        /*
         * On a non-blocking socket, recvfrom() returns <= 0 when there is
         * nothing left to read.
         */
        if (recv_len <= 0) {
            break;
        }

        g_comm.stats.packets_received++;
        g_comm.stats.bytes_received += (uint64_t)recv_len;

        process_received_message(g_comm.recv_buffer, (size_t)recv_len);

        processed++;
    }

    return processed;
}

ChronosResult_t comm_receive(MessageHeader_t* header,
                             void* payload,
                             size_t max_payload,
                             uint32_t timeout_ms)
{
    if (!header) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    uint64_t start = hal_get_time_ms();

    while ((hal_get_time_ms() - start) < timeout_ms) {
        struct sockaddr_in from;
        socklen_t from_len = sizeof(from);

        int recv_len = recvfrom(g_comm.socket_fd,
                                (char*)g_comm.recv_buffer,
                                sizeof(g_comm.recv_buffer),
                                0,
                                (struct sockaddr*)&from,
                                &from_len);

        if (recv_len > 0 &&
            recv_len >= (int)sizeof(MessageHeader_t)) {

            const MessageHeader_t* recv_header =
                (const MessageHeader_t*)g_comm.recv_buffer;

            if (memcmp(recv_header->magic,
                       PROTOCOL_MAGIC,
                       sizeof(PROTOCOL_MAGIC)) != 0) {
                continue;
            }

            memcpy(header, recv_header, sizeof(MessageHeader_t));

            if (payload && max_payload > 0) {
                size_t copy_len =
                    (size_t)(recv_len - (int)sizeof(MessageHeader_t));

                if (copy_len > max_payload) {
                    copy_len = max_payload;
                }

                memcpy(payload,
                       g_comm.recv_buffer + sizeof(MessageHeader_t),
                       copy_len);
            }

            g_comm.stats.packets_received++;
            g_comm.stats.bytes_received += (uint64_t)recv_len;

            return CHRONOS_OK;
        }

        hal_delay_ms(1);
    }

    return CHRONOS_ERR_TIMEOUT;
}

/* -------------------------------------------------------------------------
 * Network quality
 * -------------------------------------------------------------------------
 */

void comm_get_network_quality(int8_t* avg_rssi,
                              float* packet_loss_pct)
{
    if (avg_rssi) {
        int32_t total = 0;
        uint8_t count = 0;

        for (int i = 0; i < MAX_PEERS; i++) {
            if (g_comm.peers[i].active && g_comm.peers[i].rssi != 0) {
                total += g_comm.peers[i].rssi;
                count++;
            }
        }

        *avg_rssi = (count > 0) ? (int8_t)(total / count) : 0;
    }

    if (packet_loss_pct) {
        uint64_t total_sent = g_comm.stats.packets_sent + g_comm.stats.send_errors;

        if (total_sent > 0) {
            *packet_loss_pct =
                (float)g_comm.stats.send_errors * 100.0f / (float)total_sent;
        } else {
            *packet_loss_pct = 0.0f;
        }
    }
}

bool comm_is_network_healthy(void)
{
    int8_t avg_rssi = 0;
    float packet_loss = 0.0f;

    comm_get_network_quality(&avg_rssi, &packet_loss);

    /*
     * A simple rule:
     *   - RSSI is acceptable
     *   - packet loss is not too high
     *
     * This is only a rough health check, not a physical RF model.
     */
    return ((avg_rssi >= COMM_MIN_RSSI) || (avg_rssi == 0)) &&
           (packet_loss < 5.0f);
}

/* -------------------------------------------------------------------------
 * Statistics
 * -------------------------------------------------------------------------
 */

void comm_get_statistics(CommStatistics_t* stats)
{
    if (!stats) {
        return;
    }

    memcpy(stats, &g_comm.stats, sizeof(CommStatistics_t));
}

void comm_reset_statistics(void)
{
    memset(&g_comm.stats, 0, sizeof(g_comm.stats));
}