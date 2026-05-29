#ifndef COMMON_TYPES_H
#define COMMON_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/*********************************************************************
 * Generic constants
 *********************************************************************/

#define COMM_MAX_PACKET_SIZE   2048
#define COMM_RAFT_PORT         9000
#define COMM_MIN_RSSI          (-85)

#define RAFT_MAX_LOG_ENTRIES   128
#define RAFT_MAX_NODES         8

#define RAFT_HEARTBEAT_INTERVAL_MS   50
#define RAFT_ELECTION_TIMEOUT_MIN_MS 150
#define RAFT_ELECTION_TIMEOUT_MAX_MS 300

/*********************************************************************
 * Generic result codes
 *
 * Used by:
 *   - networking
 *   - RAFT
 *   - HAL
 *   - experiments
 *********************************************************************/

typedef enum {
    CHRONOS_OK = 0,

    CHRONOS_ERR_INVALID_ARG = -1,
    CHRONOS_ERR_TIMEOUT     = -2,
    CHRONOS_ERR_NO_MEMORY   = -3,
    CHRONOS_ERR_NOT_FOUND   = -4,
    CHRONOS_ERR_COMM_FAIL   = -5,
    CHRONOS_ERR_CONFLICT    = -6
} ChronosResult_t;

/*********************************************************************
 * RAFT message types
 *********************************************************************/

typedef enum {
    MSG_NONE = 0,

    MSG_REQUEST_VOTE,
    MSG_VOTE_RESPONSE,

    MSG_APPEND_ENTRIES,
    MSG_APPEND_RESPONSE,

    MSG_STATUS,
    MSG_EXPERIMENT_DATA
} MessageType_t;

/*********************************************************************
 * RAFT roles / states
 *********************************************************************/

typedef enum {
    RAFT_FOLLOWER = 0,
    RAFT_CANDIDATE,
    RAFT_LEADER
} RaftState_t;

/* Compatibility aliases used by the current source files */
#define RAFT_STATE_FOLLOWER   RAFT_FOLLOWER
#define RAFT_STATE_CANDIDATE  RAFT_CANDIDATE
#define RAFT_STATE_LEADER     RAFT_LEADER

#define MSG_TYPE_VOTE_REQ     MSG_REQUEST_VOTE
#define MSG_TYPE_VOTE_RES     MSG_VOTE_RESPONSE
#define MSG_TYPE_APPEND_REQ   MSG_APPEND_ENTRIES
#define MSG_TYPE_APPEND_RES   MSG_APPEND_RESPONSE

/*********************************************************************
 * RAFT log entry types
 *********************************************************************/

typedef enum {
    RAFT_ENTRY_TASK_ASSIGN = 0,
    RAFT_ENTRY_HEARTBEAT,
    RAFT_ENTRY_DEMO
} RaftEntryType_t;

/*********************************************************************
 * Runtime node state
 *********************************************************************/

typedef struct {
    uint8_t node_id;
    RaftState_t state;
    uint32_t current_term;
    uint8_t voted_for;
    uint8_t leader_id;
    uint32_t commit_index;
    uint32_t last_applied;
} NodeRuntimeState_t;

/*********************************************************************
 * Election statistics
 *********************************************************************/

typedef struct {
    uint32_t election_count;
    uint32_t successful_elections;
    uint32_t failed_elections;
    uint64_t average_election_time_ms;
    uint64_t longest_election_time_ms;
} ElectionStatistics_t;

/*********************************************************************
 * Experimental network impairment model
 *********************************************************************/

typedef struct {
    float packet_loss_pct;
    uint32_t fixed_delay_ms;
    uint32_t jitter_ms;
    float doppler_shift_hz;
    int8_t simulated_rssi;
} NetworkImpairment_t;

/*********************************************************************
 * Generic packet header
 *********************************************************************/

typedef struct {
    uint8_t  magic[4];
    uint16_t version;
    uint16_t type;
    uint32_t term;
    uint32_t seq_num;
    uint8_t  src_id;
    uint8_t  dst_id;
    uint32_t payload_len;
    uint32_t crc32;
} MessageHeader_t;

/*********************************************************************
 * RAFT vote request / response
 *********************************************************************/

typedef struct {
    uint32_t term;
    uint8_t candidate_id;
    uint32_t last_log_index;
    uint32_t last_log_term;
} VoteRequest_t;

typedef struct {
    uint32_t term;
    uint8_t vote_granted;
} VoteResponse_t;

/*********************************************************************
 * RAFT append entries request / response
 *********************************************************************/

typedef struct {
    uint32_t term;
    uint8_t leader_id;
    uint32_t prev_log_index;
    uint32_t prev_log_term;
    uint32_t leader_commit;
    uint32_t entry_len;
} AppendEntriesRequest_t;

typedef struct {
    uint32_t term;
    uint8_t success;
    uint32_t match_index;
} AppendEntriesResponse_t;

/*********************************************************************
 * RAFT log entry
 *********************************************************************/

typedef struct {
    uint32_t term;
    uint32_t index;
    RaftEntryType_t type;
    uint32_t data_len;
    uint8_t committed;
    char data[128];
} RaftLogEntry_t;

/*********************************************************************
 * Node information
 *********************************************************************/

typedef struct {
    uint8_t node_id;
    uint32_t ip_addr;
    uint16_t port;
    int8_t rssi;
    uint64_t last_seen;
    uint8_t is_alive;
} NodeInfo_t;

/*********************************************************************
 * Cluster state
 *********************************************************************/

typedef struct {
    uint8_t total_nodes;
    uint8_t current_leader;
    uint32_t current_term;
} ClusterInfo_t;

/*********************************************************************
 * Communication statistics
 *********************************************************************/

typedef struct {
    uint64_t packets_sent;
    uint64_t packets_received;
    uint64_t bytes_sent;
    uint64_t bytes_received;
    uint64_t send_errors;
    uint64_t receive_errors;
    uint64_t crc_errors;
    int8_t min_rssi;
    int8_t max_rssi;
} CommStatistics_t;

/*********************************************************************
 * Callback prototypes
 *********************************************************************/

typedef void (*StateChangeCallback_t)(
    RaftState_t old_state,
    RaftState_t new_state
);

typedef void (*MessageCallback_t)(
    const MessageHeader_t* header,
    const void* payload,
    size_t payload_len
);

#endif