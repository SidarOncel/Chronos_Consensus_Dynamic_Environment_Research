#ifndef CHRONOS_RAFT_H
#define CHRONOS_RAFT_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/*
 * This header is the public contract for the consensus engine.
 *
 * In the paper repo, keep all RAFT-related declarations here, and keep
 * application code (task management, path planning, safety, etc.) out of it.
 *
 * The lower-level message and shared data types are expected to come from
 * common_types.h in the full project.
 * If your current repo already defines them there, do not duplicate them here.
 */
#include "common_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * RAFT lifecycle
 * -------------------------------------------------------------------------
 *
 * These functions let the application bring the consensus engine online,
 * advance it over time, and shut it down again.
 *
 * Example:
 *   raft_init(1);
 *   while (1) {
 *       raft_tick();
 *       raft_process_messages();
 *   }
 */
ChronosResult_t raft_init(uint8_t node_id);
ChronosResult_t raft_shutdown(void);
void raft_tick(void);
uint32_t raft_process_messages(void);

/* -------------------------------------------------------------------------
 * RAFT message handlers
 * -------------------------------------------------------------------------
 *
 * These are called by the communication layer when packets arrive.
 * The network code should stay generic; RAFT decides what each message means.
 */
ChronosResult_t raft_handle_vote_request(const VoteRequest_t* req,
                                         VoteResponse_t* res);

ChronosResult_t raft_handle_vote_response(const VoteResponse_t* res);

ChronosResult_t raft_handle_append_entries(const AppendEntriesRequest_t* req,
                                           AppendEntriesResponse_t* res);

ChronosResult_t raft_handle_append_response(const AppendEntriesResponse_t* res,
                                            uint8_t from_id);

/* -------------------------------------------------------------------------
 * Log / workload interface
 * -------------------------------------------------------------------------
 *
 * The paper version should keep this generic.
 * That means the leader can append a small demo entry or a simple workload
 * item, and the commit callback can measure how long it took to confirm it.
 */
ChronosResult_t raft_append_entry(RaftEntryType_t type,
                                  const void* data,
                                  size_t data_len);

/* -------------------------------------------------------------------------
 * State accessors
 * -------------------------------------------------------------------------
 *
 * These functions are used by the main loop and the status printer.
 * They let the application inspect consensus progress without reaching
 * into raft.c internals directly.
 */
uint32_t raft_get_commit_index(void);
uint32_t raft_get_last_applied(void);
RaftState_t raft_get_state(void);
uint32_t raft_get_current_term(void);
uint8_t raft_get_leader_id(void);
bool raft_is_leader(void);
uint8_t raft_get_node_id(void);

/* -------------------------------------------------------------------------
 * Cluster management
 * -------------------------------------------------------------------------
 *
 * The paper repo should stay small, but the interface still needs to support
 * adding and removing nodes so the cluster size can be controlled during
 * experiments.
 */
ChronosResult_t raft_add_node(uint8_t node_id, const NodeInfo_t* info);
ChronosResult_t raft_remove_node(uint8_t node_id);
ChronosResult_t raft_get_cluster_info(ClusterInfo_t* info);
uint8_t raft_get_cluster_size(void);

/* -------------------------------------------------------------------------
 * Callbacks
 * -------------------------------------------------------------------------
 *
 * These are the two most useful hooks for your experiments:
 *   - state callback: tells you when the node becomes follower/candidate/leader
 *   - commit callback: tells you when a log entry has been confirmed
 *
 * This is where you measure election latency and consensus latency.
 */
void raft_set_state_callback(StateChangeCallback_t callback);
void raft_set_commit_callback(void (*callback)(const RaftLogEntry_t* entry));

/* -------------------------------------------------------------------------
 * Optional statistics
 * -------------------------------------------------------------------------
 *
 * These counters are useful when you want to write the evaluation section.
 * They let you report how many elections started, how many were won, and
 * how much progress the cluster made.
 */
typedef struct {
    uint32_t elections_started;
    uint32_t elections_won;
    uint32_t votes_granted;
    uint32_t heartbeats_sent;
    uint32_t heartbeats_received;
    uint32_t entries_committed;
    uint64_t last_election_time;
    uint64_t leader_uptime;
} RaftStatistics_t;

void raft_get_statistics(RaftStatistics_t* stats);
void raft_reset_statistics(void);

#ifdef __cplusplus
}
#endif

#endif /* CHRONOS_RAFT_H */