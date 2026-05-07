#include "network.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* -------------------------------------------------------------------------
 * Platform-specific socket includes
 * -------------------------------------------------------------------------
 *
 * Windows and Linux use different socket APIs.
 *
 * We hide those differences here so the rest of the code can stay portable.
 */

#ifdef _WIN32

    #define WIN32_LEAN_AND_MEAN
    #include <winsock2.h>
    #include <ws2tcpip.h>

    #pragma comment(lib, "ws2_32.lib")

    typedef SOCKET socket_t;

    #define CLOSE_SOCKET closesocket
    #define SOCKET_INVALID INVALID_SOCKET

#else

    #include <unistd.h>
    #include <fcntl.h>
    #include <arpa/inet.h>
    #include <sys/socket.h>
    #include <netinet/in.h>

    typedef int socket_t;

    #define CLOSE_SOCKET close
    #define SOCKET_INVALID (-1)

#endif

/* -------------------------------------------------------------------------
 * Internal constants
 * -------------------------------------------------------------------------
 */

#define MAX_PEERS       16
#define MAX_CALLBACKS   16

/*
 * Simple protocol identifier.
 *
 * If random garbage reaches the socket,
 * this helps us reject invalid packets immediately.
 */
static const uint8_t PROTOCOL_MAGIC[4] = {
    'C', 'H', 'R', 'N'
};

#define PROTOCOL_VERSION 1

/* -------------------------------------------------------------------------
 * Peer table
 * -------------------------------------------------------------------------
 *
 * Each entry describes another node in the cluster.
 *
 * Example:
 *   node 2 -> 192.168.1.22:8002
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
 * Callback table
 * -------------------------------------------------------------------------
 *
 * Different packet types trigger different handlers.
 *
 * Example:
 *   vote request -> RAFT vote handler
 *   append entry -> append handler
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
 * This struct contains the entire network subsystem state.
 *
 * Keeping it centralized simplifies debugging and statistics collection.
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
 * Optional external override.
 *
 * Useful when running multiple nodes on one PC.
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

static void process_received_message(const void* data,
                                     size_t len);

/* -------------------------------------------------------------------------
 * Initialization
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_init(uint8_t node_id,
                          const char* ip_addr)
{
    if (g_comm.initialized) {
        return CHRONOS_OK;
    }

#ifdef _WIN32

    /*
     * Windows sockets require explicit startup.
     */
    WSADATA wsa;

    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        return CHRONOS_ERR_COMM_FAIL;
    }

#endif

    memset(&g_comm, 0, sizeof(g_comm));

    g_comm.node_id = node_id;

    strncpy(g_comm.local_ip,
            ip_addr ? ip_addr : "127.0.0.1",
            sizeof(g_comm.local_ip) - 1);

    /*
     * Allow custom port override for local multi-node experiments.
     */
    g_comm.port = (g_comm_port_override != 0)
                    ? g_comm_port_override
                    : COMM_RAFT_PORT;

    ChronosResult_t result = socket_init();

    if (result != CHRONOS_OK) {
        return result;
    }

    g_comm.initialized = true;

    hal_console_log(
        "[NET] initialized node=%u port=%u",
        g_comm.node_id,
        g_comm.port
    );

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Shutdown
 * -------------------------------------------------------------------------
 */

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
     * UDP is enough for RAFT experiments.
     *
     * Why UDP instead of TCP?
     *   - lower overhead
     *   - easier to inject impairments later
     *   - closer to real unreliable transport conditions
     */

    g_comm.socket_fd =
        socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (g_comm.socket_fd == SOCKET_INVALID) {
        return CHRONOS_ERR_COMM_FAIL;
    }

    if (!socket_set_nonblocking(g_comm.socket_fd)) {

        CLOSE_SOCKET(g_comm.socket_fd);

        return CHRONOS_ERR_COMM_FAIL;
    }

    /*
     * Allow quick rebinding after restart.
     */
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

        return CHRONOS_ERR_COMM_FAIL;
    }

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Cleanup socket
 * -------------------------------------------------------------------------
 */

static void socket_cleanup(void)
{
    if (g_comm.socket_fd != SOCKET_INVALID) {

        CLOSE_SOCKET(g_comm.socket_fd);

        g_comm.socket_fd = SOCKET_INVALID;
    }
}

/* -------------------------------------------------------------------------
 * Non-blocking socket mode
 * -------------------------------------------------------------------------
 *
 * Why non-blocking?
 *
 * RAFT should continue running even if:
 *   - no packets arrive,
 *   - a peer disconnects,
 *   - the network becomes unstable.
 */

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

    return fcntl(sock,
                 F_SETFL,
                 flags | O_NONBLOCK) == 0;

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
    for (int i = 0; i < MAX_PEERS; i++) {

        if (!g_comm.peers[i].active) {

            g_comm.peers[i].active = true;

            g_comm.peers[i].node_id = node_id;

            strncpy(g_comm.peers[i].ip_addr,
                    ip_addr,
                    sizeof(g_comm.peers[i].ip_addr) - 1);

            g_comm.peers[i].port = port;

            hal_console_log(
                "[NET] peer added node=%u %s:%u",
                node_id,
                ip_addr,
                port
            );

            return CHRONOS_OK;
        }
    }

    return CHRONOS_ERR_NO_MEMORY;
}

/* -------------------------------------------------------------------------
 * Callback registration
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_register_callback(
    MessageType_t type,
    MessageCallback_t callback)
{
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

/* -------------------------------------------------------------------------
 * Send message to one node
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_send_to_node(
    uint8_t dst_id,
    MessageType_t type,
    const void* payload,
    size_t payload_len)
{
    PeerEntry_t* peer = find_peer(dst_id);

    if (!peer) {
        return CHRONOS_ERR_NOT_FOUND;
    }

    /*
     * Build packet:
     *   [header][payload]
     */

    uint8_t buffer[COMM_MAX_PACKET_SIZE];

    MessageHeader_t* header =
        (MessageHeader_t*)buffer;

    memcpy(header->magic,
           PROTOCOL_MAGIC,
           sizeof(PROTOCOL_MAGIC));

    header->version = PROTOCOL_VERSION;

    header->src_id = g_comm.node_id;

    header->dst_id = dst_id;

    header->type = (uint16_t)type;

    header->seq_num = g_comm.sequence_number++;

    header->payload_len = (uint32_t)payload_len;

    /*
     * Copy payload after the header.
     */

    if (payload && payload_len > 0) {

        memcpy(buffer + sizeof(MessageHeader_t),
               payload,
               payload_len);
    }

    /*
     * Prepare destination socket address.
     */

    struct sockaddr_in dest;

    memset(&dest, 0, sizeof(dest));

    dest.sin_family = AF_INET;

    dest.sin_port = htons(peer->port);

    inet_pton(AF_INET,
              peer->ip_addr,
              &dest.sin_addr);

    size_t total_len =
        sizeof(MessageHeader_t) + payload_len;

    int sent =
        sendto(g_comm.socket_fd,
               (const char*)buffer,
               (int)total_len,
               0,
               (struct sockaddr*)&dest,
               sizeof(dest));

    if (sent < 0) {

        g_comm.stats.send_errors++;

        return CHRONOS_ERR_COMM_FAIL;
    }

    g_comm.stats.packets_sent++;

    g_comm.stats.bytes_sent += sent;

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Broadcast helper
 * -------------------------------------------------------------------------
 */

ChronosResult_t comm_broadcast(
    MessageType_t type,
    const void* payload,
    size_t payload_len)
{
    for (int i = 0; i < MAX_PEERS; i++) {

        if (!g_comm.peers[i].active) {
            continue;
        }

        /*
         * Do not send to ourselves.
         */
        if (g_comm.peers[i].node_id ==
            g_comm.node_id) {
            continue;
        }

        comm_send_to_node(
            g_comm.peers[i].node_id,
            type,
            payload,
            payload_len
        );
    }

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Poll for packets
 * -------------------------------------------------------------------------
 *
 * This is the heart of the event loop.
 *
 * The node repeatedly:
 *   - checks for packets,
 *   - dispatches callbacks,
 *   - continues running RAFT.
 */

uint32_t comm_poll(uint32_t timeout_ms)
{
    (void)timeout_ms;

    uint32_t processed = 0;

    while (1) {

        struct sockaddr_in from;

        socklen_t from_len = sizeof(from);

        int recv_len =
            recvfrom(g_comm.socket_fd,
                     (char*)g_comm.recv_buffer,
                     sizeof(g_comm.recv_buffer),
                     0,
                     (struct sockaddr*)&from,
                     &from_len);

        /*
         * Non-blocking socket:
         * no more packets available.
         */
        if (recv_len <= 0) {
            break;
        }

        g_comm.stats.packets_received++;

        g_comm.stats.bytes_received += recv_len;

        process_received_message(
            g_comm.recv_buffer,
            (size_t)recv_len
        );

        processed++;
    }

    return processed;
}

/* -------------------------------------------------------------------------
 * Packet dispatch
 * -------------------------------------------------------------------------
 */

static void process_received_message(
    const void* data,
    size_t len)
{
    if (len < sizeof(MessageHeader_t)) {

        g_comm.stats.receive_errors++;

        return;
    }

    const MessageHeader_t* header =
        (const MessageHeader_t*)data;

    /*
     * Reject invalid protocol packets.
     */

    if (memcmp(header->magic,
               PROTOCOL_MAGIC,
               sizeof(PROTOCOL_MAGIC)) != 0) {

        g_comm.stats.receive_errors++;

        return;
    }

    const void* payload =
        (const uint8_t*)data + sizeof(MessageHeader_t);

    size_t payload_len =
        len - sizeof(MessageHeader_t);

    /*
     * Find matching callback.
     */

    for (int i = 0; i < MAX_CALLBACKS; i++) {

        if (!g_comm.callbacks[i].active) {
            continue;
        }

        if (g_comm.callbacks[i].type !=
            (MessageType_t)header->type) {
            continue;
        }

        /*
         * Dispatch packet to registered handler.
         */

        g_comm.callbacks[i].callback(
            header,
            payload,
            payload_len
        );
    }
}

/* -------------------------------------------------------------------------
 * Statistics helpers
 * -------------------------------------------------------------------------
 */

void comm_get_statistics(CommStatistics_t* stats)
{
    if (!stats) {
        return;
    }

    memcpy(stats,
           &g_comm.stats,
           sizeof(CommStatistics_t));
}

void comm_reset_statistics(void)
{
    memset(&g_comm.stats,
           0,
           sizeof(g_comm.stats));
}