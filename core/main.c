#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "raft.h"
#include "network.h"
#include "hal.h"

/*
 * Paper-version main.c
 *
 * This file is intentionally much smaller than the AGV project main.c.
 *
 * In the full AGV stack, main.c also handles:
 *   - safety monitoring
 *   - task management
 *   - path planning
 *   - robot workload generation
 *
 * For the paper repo, we only keep the pieces that matter for consensus:
 *   - timing
 *   - message passing
 *   - RAFT state transitions
 *   - commit/election logging
 *
 * That makes the experiment easier to explain and easier to reproduce.
 */

/* -------------------------------------------------------------------------
 * Demo cluster setup
 * -------------------------------------------------------------------------
 *
 * We keep the cluster small on purpose.
 * Three nodes are enough to demonstrate leader election and re-election.
 *
 * If you want to run multiple processes on one machine, the simplest trick is:
 *   node 1 -> port 8001
 *   node 2 -> port 8002
 *   node 3 -> port 8003
 *
 * That way the program behaves like a tiny cluster even if it is running
 * on the same computer or board.
 */
#define BASE_PORT 8000
#define DEMO_CLUSTER_SIZE 3

static const uint8_t demo_cluster[DEMO_CLUSTER_SIZE] = { 1, 2, 3 };

/* -------------------------------------------------------------------------
 * Small experiment payload
 * -------------------------------------------------------------------------
 *
 * This is not a robot task anymore.
 * In the paper branch it is just a generic log entry that helps us measure
 * how long it takes from "leader created the entry" to "entry committed".
 *
 * Think of it like a stamped note:
 *   - sequence number: which entry this is
 *   - created_at_ms: when the leader wrote it
 *   - label: human-readable text for debugging
 */
typedef struct {
    uint32_t sequence;
    uint64_t created_at_ms;
    char     label[48];
} DemoEntry_t;

/*
 * If your RAFT enum already has a generic type, use that instead.
 * In the current project, the task type exists already, so we reuse it
 * as a placeholder for a generic demo entry.
 *
 * In other words:
 *   the name is legacy,
 *   but the payload is just a simple demo message.
 */
#ifndef RAFT_ENTRY_DEMO
#define RAFT_ENTRY_DEMO RAFT_ENTRY_TASK_ASSIGN
#endif

/* -------------------------------------------------------------------------
 * Runtime state used only by this demo harness
 * -------------------------------------------------------------------------
 *
 * These variables do not belong in raft.c because they are not part of the
 * consensus algorithm itself.
 *
 * They exist only so the experiment can print useful measurements.
 */
static uint8_t  g_node_id = 1;
static char     g_node_ip[16] = "127.0.0.1";
static uint16_t g_node_port = 8001;

static uint64_t g_election_start_ms = 0;
static uint64_t g_last_append_ms = 0;
static uint64_t g_last_status_ms = 0;
static uint32_t g_next_sequence = 1;
static uint32_t g_total_commits = 0;

/* -------------------------------------------------------------------------
 * Helper: convert RAFT role to text
 * -------------------------------------------------------------------------
 *
 * This is only for logging.
 * Numbers are harder to read in console output, so this keeps the logs human.
 */
static const char *role_str(RaftState_t state)
{
    switch (state) {
        case RAFT_STATE_FOLLOWER:  return "FOLLOWER";
        case RAFT_STATE_CANDIDATE: return "CANDIDATE";
        case RAFT_STATE_LEADER:    return "LEADER";
        default:                   return "?";
    }
}

/* -------------------------------------------------------------------------
 * RAFT message callbacks
 * -------------------------------------------------------------------------
 *
 * These functions are the bridge between the transport layer and RAFT.
 *
 * Think of them like a mailroom:
 *   - network layer receives a message
 *   - callback decodes what kind of message it is
 *   - RAFT decides how to react
 *   - response is sent back if needed
 */

static void on_vote_request(const MessageHeader_t *header,
                            const void *payload,
                            size_t payload_len)
{
    (void)payload_len;

    const VoteRequest_t *req = (const VoteRequest_t *)payload;
    VoteResponse_t res;
    memset(&res, 0, sizeof(res));

    /*
     * RAFT decides whether the vote should be granted.
     * If it says yes, we send the vote response back to the sender.
     */
    if (raft_handle_vote_request(req, &res) == CHRONOS_OK) {
        comm_send_to_node(header->src_id,
                          MSG_TYPE_VOTE_RES,
                          &res,
                          sizeof(res));
    }
}

static void on_vote_response(const MessageHeader_t *header,
                             const void *payload,
                             size_t payload_len)
{
    (void)header;
    (void)payload_len;

    const VoteResponse_t *res = (const VoteResponse_t *)payload;

    /*
     * If this node is currently a candidate, each granted vote moves it
     * closer to becoming leader.
     */
    raft_handle_vote_response(res);
}

static void on_append_entries(const MessageHeader_t *header,
                              const void *payload,
                              size_t payload_len)
{
    (void)payload_len;

    AppendEntriesResponse_t res;
    memset(&res, 0, sizeof(res));

    /*
     * AppendEntries is the message the leader uses for:
     *   - heartbeats
     *   - log replication
     *
     * The follower checks the request, updates its timer, and sends back
     * an acknowledgment if the append is accepted.
     */
    if (raft_handle_append_entries((const AppendEntriesRequest_t *)payload, &res) == CHRONOS_OK) {
        comm_send_to_node(header->src_id,
                          MSG_TYPE_APPEND_RES,
                          &res,
                          sizeof(res));
    }
}

static void on_append_response(const MessageHeader_t *header,
                               const void *payload,
                               size_t payload_len)
{
    (void)payload_len;

    const AppendEntriesResponse_t *res = (const AppendEntriesResponse_t *)payload;

    /*
     * When the leader receives acknowledgments, it uses them to decide
     * whether an entry can be considered replicated by a majority.
     */
    hal_console_log("[COMM] Node %u received ACK from Node %u (success=%d)",
                    g_node_id,
                    header->src_id,
                    res->success);

    raft_handle_append_response(res, header->src_id);
}

/* -------------------------------------------------------------------------
 * RAFT state change callback
 * -------------------------------------------------------------------------
 *
 * This callback is useful because it gives us a clean place to mark:
 *   - when an election starts
 *   - when a leader is chosen
 *
 * That is exactly the kind of data your professor is asking for.
 */
static void on_raft_state_change(RaftState_t old_state, RaftState_t new_state)
{
    const char *states[] = { "FOLLOWER", "CANDIDATE", "LEADER" };

    hal_console_log("[RAFT] State changed: %s -> %s",
                    states[old_state],
                    states[new_state]);

    /*
     * The moment we enter candidate state is the moment the election begins.
     * We store that timestamp so we can later compute election latency.
     */
    if (new_state == RAFT_STATE_CANDIDATE) {
        g_election_start_ms = hal_get_time_ms();
        hal_console_log("[PERF] Election started at %llu ms",
                        (unsigned long long)g_election_start_ms);
    }

    /*
     * When we become leader, the difference between now and the stored
     * candidate-start timestamp is the election latency.
     */
    if (new_state == RAFT_STATE_LEADER) {
        uint64_t now = hal_get_time_ms();
        uint64_t latency = now - g_election_start_ms;

        hal_console_log("[RAFT] This node became the LEADER.");
        hal_console_log("[PERF] Election latency: %llu ms",
                        (unsigned long long)latency);
    }
}

/* -------------------------------------------------------------------------
 * Commit callback
 * -------------------------------------------------------------------------
 *
 * This is where we turn a log commit into a measurable result.
 *
 * In the paper repo, the log entry is just a generic demo payload.
 * The important part is not the payload contents.
 * The important part is:
 *   - when the leader created the entry
 *   - when the system finally committed it
 *   - how long that took
 */
static void on_log_committed(const RaftLogEntry_t *entry)
{
    g_total_commits++;

    uint64_t now = hal_get_time_ms();

    hal_console_log("[RAFT] ENTRY COMMITTED: index=%u, type=%d, term=%u",
                    entry->index,
                    entry->type,
                    entry->term);

    /*
     * The commit callback receives the actual log entry.
     * We treat the payload as our small demo record.
     * If the payload is large enough, we can recover the timestamp
     * and compute end-to-end consensus latency.
     */
    if (entry->data_len >= sizeof(DemoEntry_t)) {
        const DemoEntry_t *demo = (const DemoEntry_t *)entry->data;

        uint64_t latency = now - demo->created_at_ms;

        hal_console_log("[APP] Committed demo entry %u: %s",
                        demo->sequence,
                        demo->label);

        hal_console_log("[PERF] Consensus latency: %llu ms | total commits=%u",
                        (unsigned long long)latency,
                        g_total_commits);
    } else {
        /*
         * If the payload is smaller than expected, we still log the commit.
         * That keeps the system robust even if the payload format changes.
         */
        hal_console_log("[APP] Commit received, but payload is too small to decode.");
    }
}

/* -------------------------------------------------------------------------
 * Register all RAFT message handlers
 * -------------------------------------------------------------------------
 *
 * This is the glue between the communication layer and RAFT.
 * Without these registrations, the network layer would receive packets
 * but nobody would know what to do with them.
 */
static ChronosResult_t register_raft_callbacks(void)
{
    ChronosResult_t result;

    result = comm_register_callback(MSG_TYPE_VOTE_REQ, on_vote_request);
    if (result != CHRONOS_OK) return result;

    result = comm_register_callback(MSG_TYPE_VOTE_RES, on_vote_response);
    if (result != CHRONOS_OK) return result;

    result = comm_register_callback(MSG_TYPE_APPEND_REQ, on_append_entries);
    if (result != CHRONOS_OK) return result;

    result = comm_register_callback(MSG_TYPE_APPEND_RES, on_append_response);
    if (result != CHRONOS_OK) return result;

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Register the demo peers
 * -------------------------------------------------------------------------
 *
 * For the paper repo, we keep the cluster small and simple.
 * Three nodes are enough to show:
 *   - one leader
 *   - two followers
 *   - vote exchange
 *   - heartbeats
 *
 * If you later want a larger experiment, this is the first place to expand.
 */
static void register_demo_cluster(void)
{
    for (size_t i = 0; i < DEMO_CLUSTER_SIZE; i++) {
        uint8_t peer_id = demo_cluster[i];

        /*
         * A node never registers itself as its own peer.
         * That would be like sending a letter to your own mailbox and
         * counting it as network traffic.
         */
        if (peer_id == g_node_id) {
            continue;
        }

        uint16_t peer_port = (uint16_t)(BASE_PORT + peer_id);

        /*
         * Register the peer in the communication layer.
         * For the paper branch, using localhost is fine for the first runs.
         * Later you can replace 127.0.0.1 with real board IPs.
         */
        comm_register_peer(peer_id, "127.0.0.1", peer_port);

        /*
         * RAFT also needs to know that this peer exists in the cluster.
         * That lets the consensus logic count the majority correctly.
         */
        NodeInfo_t info;
        memset(&info, 0, sizeof(info));
        info.node_id = peer_id;
        info.port = peer_port;
        info.is_alive = 1;

        raft_add_node(peer_id, &info);
    }
}

/* -------------------------------------------------------------------------
 * Print status periodically
 * -------------------------------------------------------------------------
 *
 * This is a lightweight way to watch the system while it runs.
 * It is not the experiment itself.
 * It just helps you see whether the node is stable, leader, or still
 * waiting for an election.
 */
static void print_status(void)
{
    uint64_t now = hal_get_time_ms();

    /*
     * Print once every 5 seconds.
     * If we print every loop iteration, the output becomes noisy and hard
     * to read, especially when multiple nodes are running.
     */
    if (now - g_last_status_ms < 5000) {
        return;
    }

    g_last_status_ms = now;

    RaftState_t state = raft_get_state();
    uint32_t term = raft_get_current_term();
    uint8_t leader = raft_get_leader_id();
    uint8_t cluster = raft_get_cluster_size();

    hal_console_log("[STATUS] state=%s term=%u leader=%u cluster=%u",
                    role_str(state),
                    term,
                    leader,
                    cluster);
}

/* -------------------------------------------------------------------------
 * Optional: append a generic demo entry when we are leader
 * -------------------------------------------------------------------------
 *
 * This gives us a second metric:
 *   - not only election latency,
 *   - but also commit latency.
 *
 * Every few seconds, the leader creates one tiny entry.
 * When that entry gets committed, the commit callback prints the latency.
 */
static void maybe_append_demo_entry(void)
{
    if (!raft_is_leader()) {
        return;
    }

    uint64_t now = hal_get_time_ms();

    /*
     * Only append every few seconds.
     * That keeps the logs readable and avoids flooding the cluster.
     */
    if (now - g_last_append_ms < 5000) {
        return;
    }

    DemoEntry_t demo;
    memset(&demo, 0, sizeof(demo));

    demo.sequence = g_next_sequence++;
    demo.created_at_ms = now;

    /*
     * This label is just for human readability.
     * Think of it like writing a note on a packet so you know what it was.
     */
    snprintf(demo.label, sizeof(demo.label),
             "demo consensus entry %u", demo.sequence);

    /*
     * In the current project, RAFT_ENTRY_TASK_ASSIGN already exists,
     * so we reuse it here as a generic placeholder.
     *
     * If you later add a dedicated generic enum value, replace it here.
     */
    if (raft_append_entry(RAFT_ENTRY_DEMO, &demo, sizeof(demo)) == CHRONOS_OK) {
        g_last_append_ms = now;
        hal_console_log("[LEADER] Appended demo entry %u", demo.sequence);
    }
}

/* -------------------------------------------------------------------------
 * Main entry point
 * -------------------------------------------------------------------------
 *
 * This is the smallest useful executable for the paper repo.
 * It runs the consensus stack, prints state changes, and generates data.
 */
int main(int argc, char **argv)
{
    /*
     * Optional command-line argument:
     *   argv[1] = node ID
     *
     * Example:
     *   ./raft_test 1
     *   ./raft_test 2
     *   ./raft_test 3
     *
     * That makes it easy to run several processes on the same machine.
     */
    if (argc > 1) {
        g_node_id = (uint8_t)atoi(argv[1]);
        g_node_port = (uint16_t)(BASE_PORT + g_node_id);
    }

    /*
     * HAL is the first thing to initialize because everything else depends
     * on time and logging.
     */
    if (hal_init() != CHRONOS_OK) {
        fprintf(stderr, "HAL init failed.\n");
        return -1;
    }

    hal_console_log("CHRONOS consensus demo starting...");
    hal_console_log("Node ID: %u | Port: %u", g_node_id, g_node_port);

    /*
     * Communication layer:
     * this is where UDP sockets, peers, callbacks, and packet handling live.
     */
    if (comm_init(g_node_id, g_node_ip) != CHRONOS_OK) {
        hal_console_log("Communication init failed.");
        return -1;
    }

    /*
     * RAFT initialization:
     * this creates the local consensus state for this node.
     */
    if (raft_init(g_node_id) != CHRONOS_OK) {
        hal_console_log("RAFT init failed.");
        return -1;
    }

    /*
     * Register callbacks after RAFT is ready.
     * This is the moment when the network layer and consensus layer start
     * talking to each other.
     */
    raft_set_state_callback(on_raft_state_change);
    raft_set_commit_callback(on_log_committed);

    if (register_raft_callbacks() != CHRONOS_OK) {
        hal_console_log("Failed to register RAFT callbacks.");
        return -1;
    }

    register_demo_cluster();

    hal_console_log("Initialization complete. Entering main loop...");
    hal_console_log("------------------------------------------------------------");

    /*
     * Main loop:
     *   1. tick RAFT
     *   2. process incoming messages
     *   3. print status
     *   4. append a demo entry if leader
     *   5. wait a little
     *
     * This loop is intentionally simple.
     * In the paper repo, simplicity is a feature because it makes the
     * experiment easier to understand and reproduce.
     */
    while (1) {
        raft_tick();

        /*
         * Process queued network messages.
         * raft_process_messages() already wraps the communication poll,
         * so we do not call comm_poll() again here.
         */
        raft_process_messages();

        print_status();
        maybe_append_demo_entry();

        /*
         * Small sleep so the loop does not consume 100% CPU and so the
         * timestamps are easier to interpret in the log.
         */
        hal_delay_ms(10);
    }

    /*
     * This cleanup is here for completeness.
     * In practice, an embedded demo often runs forever.
     */
    raft_shutdown();
    comm_shutdown();

    return 0;
}