#ifndef CHRONOS_NETWORK_H
#define CHRONOS_NETWORK_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/*
 * The network layer is the bridge between the consensus algorithm and the
 * actual transport system underneath it.
 *
 * In simple terms:
 *   RAFT decides what it wants to send,
 *   network.c decides how to send it,
 *   and the operating system / socket API does the low-level work.
 *
 * This separation matters because it lets the same RAFT logic run on:
 *   - localhost for experiments,
 *   - a Milk-V board,
 *   - or later on a more realistic network setup.
 */

#include "common_types.h"
#include "hal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * Module configuration
 * -------------------------------------------------------------------------
 *
 * These constants keep the paper repo small and predictable.
 * You can change them later if you want a larger cluster or more callbacks.
 */
#define COMM_MAX_PACKET_SIZE   2048
#define COMM_RAFT_PORT         8000
#define COMM_MIN_RSSI         (-85)

/*
 * In the full project, the communication layer tracks peers, callbacks,
 * sequence numbers, packet statistics, and quality indicators.
 * The struct definitions below are kept public only if your code needs to
 * inspect them directly.
 *
 * If your common_types.h already defines these types, keep only the function
 * declarations here and remove duplicate struct definitions.
 */

/* -------------------------------------------------------------------------
 * Message callback type
 * -------------------------------------------------------------------------
 *
 * A callback is a function that runs when a packet of a specific type arrives.
 *
 * Example:
 *   - vote request arrives -> call RAFT vote handler
 *   - append entries arrives -> call RAFT append handler
 *
 * This is how the network layer stays generic while RAFT stays focused.
 */
typedef void (*MessageCallback_t)(const MessageHeader_t* header,
                                   const void* payload,
                                   size_t payload_len);

/* -------------------------------------------------------------------------
 * Initialization / shutdown
 * -------------------------------------------------------------------------
 *
 * comm_init() brings up the transport layer.
 * comm_shutdown() closes sockets and clears the module state.
 *
 * Example:
 *   comm_init(1, "127.0.0.1");
 *   ...
 *   comm_shutdown();
 */
ChronosResult_t comm_init(uint8_t node_id, const char* ip_addr);
ChronosResult_t comm_shutdown(void);

/* -------------------------------------------------------------------------
 * Peer management
 * -------------------------------------------------------------------------
 *
 * The communication layer keeps a small table of peers.
 * Each peer represents another node in the cluster.
 *
 * Example:
 *   comm_register_peer(2, "127.0.0.1", 8002);
 *   comm_register_peer(3, "127.0.0.1", 8003);
 *
 * That gives the node enough information to send RAFT messages to them.
 */
ChronosResult_t comm_register_peer(uint8_t node_id,
                                   const char* ip_addr,
                                   uint16_t port);

ChronosResult_t comm_unregister_peer(uint8_t node_id);
ChronosResult_t comm_get_peer_info(uint8_t node_id, NodeInfo_t* info);

/*
 * Update peer RSSI / link quality estimate.
 *
 * In the paper repo this can be used as a simple signal-quality hook.
 * Later, if you introduce SDR-based traces, this becomes a natural place
 * to inject time-varying link quality.
 */
void comm_update_peer_rssi(uint8_t node_id, int8_t rssi);

/* -------------------------------------------------------------------------
 * Message sending
 * -------------------------------------------------------------------------
 *
 * These are the main functions RAFT uses to communicate.
 *
 * send_to_node():
 *   send a packet to one specific peer
 *
 * broadcast():
 *   send the same packet to all active peers
 *
 * raw_udp():
 *   lower-level helper when you want to send a packet without RAFT framing
 *
 * send_message():
 *   send an already prepared message header + payload
 */
ChronosResult_t comm_send_to_node(uint8_t dst_id,
                                  MessageType_t type,
                                  const void* payload,
                                  size_t payload_len);

ChronosResult_t comm_broadcast(MessageType_t type,
                               const void* payload,
                               size_t payload_len);

ChronosResult_t comm_send_raw_udp(const char* ip_addr,
                                  uint16_t port,
                                  const void* data,
                                  size_t len);

ChronosResult_t comm_send_message(const MessageHeader_t* header,
                                  const void* payload);

/* -------------------------------------------------------------------------
 * Message receiving
 * -------------------------------------------------------------------------
 *
 * The communication layer can work in two styles:
 *   1. callback-driven polling
 *   2. blocking receive with timeout
 *
 * The paper repo mainly uses polling because it is easier to integrate into
 * the main loop.
 */
ChronosResult_t comm_register_callback(MessageType_t type,
                                       MessageCallback_t callback);

ChronosResult_t comm_unregister_callback(MessageType_t type);

/*
 * Poll the socket and process all pending messages.
 *
 * Example:
 *   while (1) {
 *       raft_tick();
 *       comm_poll(0);
 *   }
 *
 * timeout_ms is kept for API completeness, but the paper version can use
 * non-blocking polling behavior.
 */
uint32_t comm_poll(uint32_t timeout_ms);

/*
 * Receive a single packet with timeout.
 *
 * This is useful when you want a direct receive path instead of callback
 * dispatching.
 */
ChronosResult_t comm_receive(MessageHeader_t* header,
                             void* payload,
                             size_t max_payload,
                             uint32_t timeout_ms);

/* -------------------------------------------------------------------------
 * Message integrity
 * -------------------------------------------------------------------------
 *
 * CRC is used to detect accidental corruption.
 * In networking experiments, CRC is useful because it gives a clean way
 * to reject malformed or damaged packets.
 */
uint32_t comm_calculate_crc32(const void* data, size_t len);
bool comm_verify_crc(const MessageHeader_t* header, const void* payload);

/* -------------------------------------------------------------------------
 * Network quality
 * -------------------------------------------------------------------------
 *
 * These functions are useful for logging and experiments.
 * They let you ask:
 *   - how strong is the link?
 *   - how much packet loss are we seeing?
 *   - is the network healthy enough for stable consensus?
 *
 * In the paper repo, these values can help you describe how network
 * variability affects leader election and replication.
 */
void comm_get_network_quality(int8_t* avg_rssi,
                              float* packet_loss_pct);

bool comm_is_network_healthy(void);

/* -------------------------------------------------------------------------
 * Statistics
 * -------------------------------------------------------------------------
 *
 * This is where you keep the experiment counters.
 * Typical uses:
 *   - packets sent / received
 *   - CRC failures
 *   - send errors
 *   - receive errors
 *
 * These counters are useful for tables and plots in the paper.
 */
void comm_get_statistics(CommStatistics_t* stats);
void comm_reset_statistics(void);

/* -------------------------------------------------------------------------
 * Optional port override
 * -------------------------------------------------------------------------
 *
 * This global lets the application change the default port before
 * comm_init() runs.
 *
 * Example:
 *   g_comm_port_override = 8003;
 *   comm_init(3, "127.0.0.1");
 *
 * That is handy when running several nodes on the same machine.
 */
extern uint16_t g_comm_port_override;

#ifdef __cplusplus
}
#endif

#endif /* CHRONOS_NETWORK_H */