#include "raft.h"
#include "network.h"
#include "hal.h"

#include <string.h>

/*
 * This file contains the actual consensus state machine:
 *   - follower / candidate / leader transitions
 *   - election timer handling
 *   - vote request and vote response handling
 *   - append entries / heartbeat handling
 *   - log commit advancement
 *
 * The important separation is:
 *   - hal.c  -> timing and logging
 *   - network.c -> transport and callbacks
 *   - raft.c -> consensus logic
 *
 * That separation is already present in full project.
 * The paper repo keeps the same idea, but removes AGV-specific workload
 * such as task planning and collision logic. 
 */

/* -------------------------------------------------------------------------
 * Assumed message layout
 * -------------------------------------------------------------------------
 *
 * The paper repo expects these logical fields to exist in common_types.h:
 *
 *   VoteRequest_t:
 *     - term
 *     - candidate_id
 *     - last_log_index
 *     - last_log_term
 *
 *   VoteResponse_t:
 *     - term
 *     - vote_granted
 *
 *   AppendEntriesRequest_t:
 *     - term
 *     - leader_id
 *     - prev_log_index
 *     - prev_log_term
 *     - leader_commit
 *     - entry_len
 *
 *   AppendEntriesResponse_t:
 *     - term
 *     - success
 *     - match_index
 *
 *   RaftLogEntry_t:
 *     - term
 *     - index
 *     - type
 *     - data_len
 *     - data[]
 *
 * If your common_types.h uses slightly different field names, keep the logic
 * and rename the fields accordingly.
 */

/* -------------------------------------------------------------------------
 * Tunable timings
 * -------------------------------------------------------------------------
 *
 * Election timeout and heartbeat interval are kept small and readable.
 * In a real experiment you may tune these values to match your network.
 */
#define HB_INTERVAL_MS              100U
#define ELECTION_TIMEOUT_BASE_MS    150U
#define ELECTION_TIMEOUT_JITTER_MS  150U

/* -------------------------------------------------------------------------
 * Internal consensus state
 * -------------------------------------------------------------------------
 *
 * This struct is the local memory image of the Raft node.
 * The rest of the system should not modify it directly.
 */
static struct {
    uint32_t term;

    int32_t voted_for;

    uint32_t log_cnt;
    RaftLogEntry_t log[RAFT_MAX_LOG_ENTRIES];

    RaftState_t state;

    uint32_t commit_idx;
    uint32_t last_applied;

    uint8_t leader_id;

    uint64_t timer_ms;
    uint32_t election_timeout_ms;

    /*
     * These arrays are only meaningful for the leader.
     * They track replication progress for each known peer.
     */
    uint32_t next_idx[RAFT_MAX_NODES];
    uint32_t match_idx[RAFT_MAX_NODES];

    /*
     * Local vote counter for the current election.
     */
    uint8_t votes;

} g_raft;

/* -------------------------------------------------------------------------
 * Local node identity and cluster size
 * -------------------------------------------------------------------------
 *
 * The node id is the local identity of this process.
 * Cluster size is kept separate so majority checks stay simple.
 */
static uint8_t g_my_id = 0;
static uint8_t g_cluster_size = 1;

/* -------------------------------------------------------------------------
 * Optional callbacks into the application layer
 * -------------------------------------------------------------------------
 *
 * The main program uses these callbacks to print state transitions and
 * report when a log entry is committed.
 */
static StateChangeCallback_t g_state_cb = NULL;
static void (*g_commit_cb)(const RaftLogEntry_t*) = NULL;

/* -------------------------------------------------------------------------
 * Basic statistics
 * -------------------------------------------------------------------------
 *
 * These are useful for the paper because they give you summary numbers
 * even before you build full plots.
 */
static RaftStatistics_t g_stats;

/* -------------------------------------------------------------------------
 * Extra timing markers for statistics
 * -------------------------------------------------------------------------
 *
 * We keep the election start time and leader start time separate so we can
 * report election latency and leader uptime cleanly.
 */
static uint64_t g_election_start_ms = 0;
static uint64_t g_leader_start_ms = 0;

/* -------------------------------------------------------------------------
 * Helper: convert node id to array index
 * -------------------------------------------------------------------------
 *
 * Node ids are 1-based in the demo cluster, so node 1 maps to index 0.
 */
static int idx(uint8_t id)
{
    if (id < 1 || id > RAFT_MAX_NODES) {
        return -1;
    }
    return (int)id - 1;
}

/* -------------------------------------------------------------------------
 * Helper: change RAFT state
 * -------------------------------------------------------------------------
 *
 * This function centralizes the state transition action.
 * It updates the current state and notifies the application callback.
 *
 * Why this matters:
 *   - fewer duplicated transition side effects
 *   - cleaner logging
 *   - easier to measure when the node entered a new role
 */
static void change_state(RaftState_t new_state)
{
    if (g_raft.state == new_state) {
        return;
    }

    RaftState_t old_state = g_raft.state;
    g_raft.state = new_state;

    if (g_state_cb) {
        g_state_cb(old_state, new_state);
    }
}

/* -------------------------------------------------------------------------
 * Helper: reset election / heartbeat timer
 * -------------------------------------------------------------------------
 *
 * Followers and candidates use this timer differently:
 *   - follower: wait for leader heartbeat
 *   - candidate: wait for election timeout
 *   - leader: use it to schedule periodic heartbeats
 *
 * We add random jitter so that nodes do not always time out together.
 * That reduces split-vote behavior.
 */
static void reset_timer(void)
{
    g_raft.timer_ms = hal_get_time_ms();

    /*
     * Jitter is intentionally simple.
     * The point is not cryptographic randomness.
     * The point is to avoid synchronized elections.
     */
    uint32_t jitter = hal_random_u32() % ELECTION_TIMEOUT_JITTER_MS;
    g_raft.election_timeout_ms = ELECTION_TIMEOUT_BASE_MS + jitter;
}

/* -------------------------------------------------------------------------
 * Helper: build and broadcast a vote request
 * -------------------------------------------------------------------------
 *
 * When a follower becomes a candidate, it asks every peer for a vote.
 * This is the actual trigger for leader election.
 */
static void broadcast_vote_request(void)
{
    VoteRequest_t req;
    memset(&req, 0, sizeof(req));

    req.term = g_raft.term;
    req.candidate_id = g_my_id;
    req.last_log_index = g_raft.log_cnt;
    req.last_log_term =
        (g_raft.log_cnt > 0)
            ? g_raft.log[g_raft.log_cnt - 1].term
            : 0;

    comm_broadcast(MSG_TYPE_VOTE_REQ, &req, sizeof(req));
}

/* -------------------------------------------------------------------------
 * Helper: broadcast AppendEntries
 * -------------------------------------------------------------------------
 *
 * This function is used for two different leader actions:
 *   1. heartbeat (no log entry attached)
 *   2. log replication (log entry attached)
 *
 * If entry == NULL, it is just a heartbeat.
 * If entry != NULL, the packet includes the log record after the request.
 */
static void broadcast_append_entries(const RaftLogEntry_t* entry)
{
    uint8_t packet[sizeof(AppendEntriesRequest_t) + sizeof(RaftLogEntry_t)];
    memset(packet, 0, sizeof(packet));

    AppendEntriesRequest_t* req = (AppendEntriesRequest_t*)packet;

    req->term = g_raft.term;
    req->leader_id = g_my_id;
    req->prev_log_index = g_raft.log_cnt;
    req->prev_log_term =
        (g_raft.log_cnt > 0)
            ? g_raft.log[g_raft.log_cnt - 1].term
            : 0;

    req->leader_commit = g_raft.commit_idx;

    if (entry) {
        req->entry_len = sizeof(RaftLogEntry_t);
        memcpy(packet + sizeof(AppendEntriesRequest_t),
               entry,
               sizeof(RaftLogEntry_t));
    } else {
        req->entry_len = 0;
    }

    comm_broadcast(MSG_TYPE_APPEND_REQ,
                   packet,
                   sizeof(AppendEntriesRequest_t) + req->entry_len);

    g_stats.heartbeats_sent++;
}

/* -------------------------------------------------------------------------
 * Initialization / shutdown
 * -------------------------------------------------------------------------
 */

ChronosResult_t raft_init(uint8_t id)
{
    if (idx(id) < 0) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    g_my_id = id;
    g_cluster_size = 1;

    memset(&g_raft, 0, sizeof(g_raft));
    memset(&g_stats, 0, sizeof(g_stats));

    g_raft.voted_for = -1;
    g_raft.state = RAFT_STATE_FOLLOWER;
    g_raft.leader_id = 0;

    reset_timer();

    hal_console_log("[RAFT] initialized node=%u", g_my_id);

    return CHRONOS_OK;
}

ChronosResult_t raft_shutdown(void)
{
    /*
     * Nothing heavy to free in this paper version.
     * We keep the function so the API stays complete.
     */
    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Main tick function
 * -------------------------------------------------------------------------
 *
 * This is called periodically from main().
 *
 * The logic is:
 *   - leader: send heartbeat, advance commit index
 *   - follower/candidate: if timeout expires, start an election
 *   - always apply newly committed entries
 */
void raft_tick(void)
{
    uint64_t now = hal_get_time_ms();

    if (g_raft.state == RAFT_STATE_LEADER) {

        /*
         * Leaders send periodic heartbeats.
         * A heartbeat is just an AppendEntries message with no payload.
         */
        if (now - g_raft.timer_ms >= HB_INTERVAL_MS) {
            broadcast_append_entries(NULL);
            g_raft.timer_ms = now;
        }

        /*
         * Commit index advancement:
         * if a log entry from the current term is replicated on a majority,
         * it can be committed.
         */
        for (uint32_t n = g_raft.log_cnt; n > g_raft.commit_idx; n--) {

            if (g_raft.log[n - 1].term != g_raft.term) {
                continue;
            }

            int votes = 1; /* leader counts itself */

            for (int i = 0; i < RAFT_MAX_NODES; i++) {
                if (i == idx(g_my_id)) {
                    continue;
                }

                if (g_raft.match_idx[i] >= n) {
                    votes++;
                }
            }

            if (votes > (int)(g_cluster_size / 2U)) {
                g_raft.commit_idx = n;
                break;
            }
        }
    } else {

        /*
         * Followers and candidates watch the timer.
         * If the timer expires, a new election begins.
         */
        if (now - g_raft.timer_ms >= g_raft.election_timeout_ms) {

            change_state(RAFT_STATE_CANDIDATE);

            g_stats.elections_started++;
            g_election_start_ms = now;

            g_raft.term++;
            g_raft.voted_for = (int32_t)g_my_id;
            g_raft.votes = 1; /* self-vote */

            reset_timer();

            /*
             * Ask the cluster for votes.
             * This is the core of leader election.
             */
            broadcast_vote_request();
        }
    }

    /*
     * Apply any newly committed entries in order.
     * This is where a committed log entry becomes visible to the app.
     */
    while (g_raft.last_applied < g_raft.commit_idx) {
        g_raft.last_applied++;

        if (g_commit_cb) {
            g_commit_cb(&g_raft.log[g_raft.last_applied - 1]);
        }

        g_stats.entries_committed++;
    }
}

/* -------------------------------------------------------------------------
 * Vote request handling
 * -------------------------------------------------------------------------
 *
 * A vote request asks:
 *   "Can I become leader?"
 *
 * The decision depends on:
 *   - term freshness
 *   - whether we already voted
 *   - whether the candidate's log is up to date
 */
ChronosResult_t raft_handle_vote_request(const VoteRequest_t* req,
                                         VoteResponse_t* res)
{
    if (!req || !res) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    memset(res, 0, sizeof(*res));

    /*
     * If the incoming term is newer, step down and accept it.
     */
    if (req->term > g_raft.term) {
        change_state(RAFT_STATE_FOLLOWER);
        g_raft.term = req->term;
        g_raft.voted_for = -1;
    }

    uint32_t last_term =
        (g_raft.log_cnt > 0)
            ? g_raft.log[g_raft.log_cnt - 1].term
            : 0;

    bool log_ok =
        (req->last_log_term > last_term) ||
        (req->last_log_term == last_term &&
         req->last_log_index >= g_raft.log_cnt);

    /*
     * Grant vote only if:
     *   - the term is current
     *   - we have not already voted for someone else
     *   - the candidate's log is sufficiently up to date
     */
    if (req->term >= g_raft.term &&
        (g_raft.voted_for == -1 || g_raft.voted_for == (int32_t)req->candidate_id) &&
        log_ok) {

        g_raft.voted_for = (int32_t)req->candidate_id;
        res->vote_granted = 1;
        reset_timer();
    } else {
        res->vote_granted = 0;
    }

    res->term = g_raft.term;

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Vote response handling
 * -------------------------------------------------------------------------
 *
 * Candidates count granted votes.
 * If a majority is reached, the node becomes leader.
 */
ChronosResult_t raft_handle_vote_response(const VoteResponse_t* res)
{
    if (!res) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    if (g_raft.state != RAFT_STATE_CANDIDATE) {
        return CHRONOS_OK;
    }

    if (res->term > g_raft.term) {
        /*
         * Another node knows about a newer term.
         * Step down immediately.
         */
        change_state(RAFT_STATE_FOLLOWER);
        g_raft.term = res->term;
        g_raft.voted_for = -1;
        reset_timer();
        return CHRONOS_OK;
    }

    if (res->vote_granted) {
        g_raft.votes++;
        g_stats.votes_granted++;

        if (g_raft.votes > (g_cluster_size / 2U)) {

            change_state(RAFT_STATE_LEADER);
            g_raft.leader_id = g_my_id;

            g_leader_start_ms = hal_get_time_ms();
            g_stats.elections_won++;
            g_stats.last_election_time = g_leader_start_ms - g_election_start_ms;

            /*
             * Initialize leader replication state.
             * Each follower starts with the next log index the leader expects.
             */
            for (int i = 0; i < RAFT_MAX_NODES; i++) {
                g_raft.next_idx[i] = g_raft.log_cnt + 1;
                g_raft.match_idx[i] = 0;
            }

            /*
             * Send an initial heartbeat immediately.
             * This stabilizes the cluster right after election.
             */
            broadcast_append_entries(NULL);
            g_raft.timer_ms = hal_get_time_ms();

            hal_console_log("[RAFT] node=%u became leader",
                            g_my_id);
        }
    }

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * AppendEntries request handling
 * -------------------------------------------------------------------------
 *
 * The leader uses AppendEntries for:
 *   - heartbeats
 *   - log replication
 *
 * The follower checks:
 *   - term freshness
 *   - previous log match
 *   - optional log payload
 */
ChronosResult_t raft_handle_append_entries(const AppendEntriesRequest_t* req,
                                           AppendEntriesResponse_t* res)
{
    if (!req || !res) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    memset(res, 0, sizeof(*res));

    /*
     * Reject stale leaders.
     */
    if (req->term < g_raft.term) {
        res->term = g_raft.term;
        res->success = 0;
        res->match_index = g_raft.log_cnt;
        return CHRONOS_OK;
    }

    /*
     * A newer or equal term means the sender is the current leader.
     * Followers step down and reset their election timers.
     */
    if (req->term >= g_raft.term) {
        if (g_raft.state != RAFT_STATE_FOLLOWER) {
            change_state(RAFT_STATE_FOLLOWER);
        }

        g_raft.term = req->term;
        g_raft.leader_id = req->leader_id;
        g_raft.voted_for = -1;
        reset_timer();
        g_stats.heartbeats_received++;
    }

    /*
     * Log consistency check:
     * the follower must already have the previous entry that the leader references.
     */
    if (req->prev_log_index > g_raft.log_cnt) {
        res->term = g_raft.term;
        res->success = 0;
        res->match_index = g_raft.log_cnt;
        return CHRONOS_OK;
    }

    if (req->prev_log_index > 0) {
        uint32_t local_prev_term =
            g_raft.log[req->prev_log_index - 1].term;

        if (local_prev_term != req->prev_log_term) {
            res->term = g_raft.term;
            res->success = 0;
            res->match_index = g_raft.log_cnt;
            return CHRONOS_OK;
        }
    }

    /*
     * If the packet carries a log entry, copy it into the follower log.
     * If entry_len is zero, then this was just a heartbeat.
     */
    if (req->entry_len >= sizeof(RaftLogEntry_t)) {

        const RaftLogEntry_t* entry =
            (const RaftLogEntry_t*)((const uint8_t*)req + sizeof(AppendEntriesRequest_t));

        if (g_raft.log_cnt < RAFT_MAX_LOG_ENTRIES &&
            entry->index == g_raft.log_cnt + 1) {

            g_raft.log[g_raft.log_cnt] = *entry;
            g_raft.log_cnt++;
        }
    }

    /*
     * Leader commit index can advance the follower commit index.
     */
    if (req->leader_commit > g_raft.commit_idx) {
        g_raft.commit_idx =
            (req->leader_commit < g_raft.log_cnt)
                ? req->leader_commit
                : g_raft.log_cnt;
    }

    res->term = g_raft.term;
    res->success = 1;
    res->match_index = g_raft.log_cnt;

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * AppendEntries response handling
 * -------------------------------------------------------------------------
 *
 * The leader updates match_idx/next_idx according to the result.
 * That is how it knows which followers have replicated which entry.
 */
ChronosResult_t raft_handle_append_response(const AppendEntriesResponse_t* res,
                                            uint8_t from_id)
{
    if (!res) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    if (g_raft.state != RAFT_STATE_LEADER) {
        return CHRONOS_OK;
    }

    if (res->term > g_raft.term) {
        /*
         * A follower knows about a newer term.
         * The leader must step down.
         */
        change_state(RAFT_STATE_FOLLOWER);
        g_raft.term = res->term;
        g_raft.voted_for = -1;
        reset_timer();
        return CHRONOS_OK;
    }

    int i = idx(from_id);
    if (i < 0) {
        return CHRONOS_OK;
    }

    if (res->success) {
        g_raft.match_idx[i] = res->match_index;
        g_raft.next_idx[i] = res->match_index + 1;
    } else {
        /*
         * If replication failed, back up and retry later.
         */
        if (g_raft.next_idx[i] > 1) {
            g_raft.next_idx[i]--;
        }
    }

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Append a new entry to the local log
 * -------------------------------------------------------------------------
 *
 * This is called by the application when the current node is leader.
 * The paper repo uses a generic payload rather than an AGV-specific task.
 */
ChronosResult_t raft_append_entry(RaftEntryType_t type,
                                  const void* data,
                                  size_t data_len)
{
    if (g_raft.state != RAFT_STATE_LEADER) {
        return CHRONOS_ERR_NOT_LEADER;
    }

    if (g_raft.log_cnt >= RAFT_MAX_LOG_ENTRIES) {
        return CHRONOS_ERR_NO_MEMORY;
    }

    if (data_len > sizeof(g_raft.log[0].data)) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    RaftLogEntry_t entry;
    memset(&entry, 0, sizeof(entry));

    entry.term = g_raft.term;
    entry.index = g_raft.log_cnt + 1;
    entry.type = type;
    entry.data_len = (uint32_t)data_len;

    if (data && data_len > 0) {
        memcpy(entry.data, data, data_len);
    }

    /*
     * Store locally first.
     * Then replicate to the rest of the cluster.
     */
    g_raft.log[g_raft.log_cnt++] = entry;

    /*
     * Broadcast the entry to peers.
     * The packet is built as:
     *   [AppendEntriesRequest_t][RaftLogEntry_t]
     *
     * If this later becomes too large for your experiment, you can send
     * smaller chunks or use a dedicated message format.
     */
    broadcast_append_entries(&entry);

    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Message processing bridge
 * -------------------------------------------------------------------------
 *
 * In the paper repo, this is just a thin wrapper around comm_poll().
 * The main loop calls it so the node handles incoming packets regularly.
 */
uint32_t raft_process_messages(void)
{
    return comm_poll(0);
}

/* -------------------------------------------------------------------------
 * Accessors
 * -------------------------------------------------------------------------
 *
 * These are intentionally simple.
 * The application should inspect the consensus state through these functions
 * instead of touching the internal state directly.
 */
uint32_t raft_get_commit_index(void)
{
    return g_raft.commit_idx;
}

uint32_t raft_get_last_applied(void)
{
    return g_raft.last_applied;
}

RaftState_t raft_get_state(void)
{
    return g_raft.state;
}

uint32_t raft_get_current_term(void)
{
    return g_raft.term;
}

uint8_t raft_get_leader_id(void)
{
    return g_raft.leader_id;
}

bool raft_is_leader(void)
{
    return g_raft.state == RAFT_STATE_LEADER;
}

uint8_t raft_get_node_id(void)
{
    return g_my_id;
}

/* -------------------------------------------------------------------------
 * Cluster management
 * -------------------------------------------------------------------------
 *
 * These functions let the application describe the peer set.
 * The paper repo keeps this minimal, but the interface still allows
 * experiments with cluster size changes later.
 */
ChronosResult_t raft_add_node(uint8_t node_id, const NodeInfo_t* info)
{
    (void)info;

    if (idx(node_id) < 0) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    if (g_cluster_size < RAFT_MAX_NODES) {
        g_cluster_size++;
    }

    int i = idx(node_id);
    g_raft.next_idx[i] = g_raft.log_cnt + 1;
    g_raft.match_idx[i] = 0;

    return CHRONOS_OK;
}

ChronosResult_t raft_remove_node(uint8_t node_id)
{
    if (idx(node_id) < 0) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    if (g_cluster_size > 1) {
        g_cluster_size--;
    }

    return CHRONOS_OK;
}

ChronosResult_t raft_get_cluster_info(ClusterInfo_t* info)
{
    /*
     * Paper repo version:
     * keep this function as a safe placeholder unless your ClusterInfo_t
     * has a known layout in common_types.h.
     *
     * If you already have fields in ClusterInfo_t, fill them here.
     */
    if (!info) {
        return CHRONOS_ERR_INVALID_ARG;
    }

    memset(info, 0, sizeof(*info));
    return CHRONOS_OK;
}

uint8_t raft_get_cluster_size(void)
{
    return g_cluster_size;
}

/* -------------------------------------------------------------------------
 * Callback registration
 * -------------------------------------------------------------------------
 *
 * The application uses these to get notified when:
 *   - the role changes
 *   - a log entry gets committed
 */
void raft_set_state_callback(StateChangeCallback_t callback)
{
    g_state_cb = callback;
}

void raft_set_commit_callback(void (*callback)(const RaftLogEntry_t* entry))
{
    g_commit_cb = callback;
}

/* -------------------------------------------------------------------------
 * Statistics
 * -------------------------------------------------------------------------
 *
 * These counters are useful for quick experiment summaries.
 */
void raft_get_statistics(RaftStatistics_t* stats)
{
    if (!stats) {
        return;
    }

    *stats = g_stats;

    /*
     * Leader uptime is computed live so that the number keeps increasing
     * while the current node is leader.
     */
    if (g_raft.state == RAFT_STATE_LEADER) {
        stats->leader_uptime = hal_get_time_ms() - g_leader_start_ms;
    }
}

void raft_reset_statistics(void)
{
    memset(&g_stats, 0, sizeof(g_stats));
}