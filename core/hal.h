#ifndef CHRONOS_HAL_H
#define CHRONOS_HAL_H

#include <stdint.h>
#include <stdbool.h>

/*
 * HAL = Hardware Abstraction Layer
 *
 * This file separates the consensus logic from platform-specific details.
 *
 * Why this matters:
 * -----------------
 * RAFT itself should not care whether it runs on:
 *   - a Windows laptop,
 *   - Linux PC,
 *   - Milk-V Duo,
 *   - or another RISC-V board.
 *
 * The consensus layer only asks:
 *   - "What time is it?"
 *   - "Wait a little."
 *   - "Print this message."
 *
 * HAL is responsible for answering those requests using whatever operating
 * system or hardware is available underneath.
 *
 * Think of HAL like a translator between:
 *   consensus logic  <->  operating system / hardware
 */

/* -------------------------------------------------------------------------
 * Generic result codes
 * -------------------------------------------------------------------------
 *
 * We keep the result enum very small in the paper repo.
 * The large AGV project may contain many more detailed error types.
 */
typedef enum {
    CHRONOS_OK = 0,
    CHRONOS_ERR_INVALID_ARG = -1,
    CHRONOS_ERR_TIMEOUT = -2,
    CHRONOS_ERR_COMM_FAIL = -3,
    CHRONOS_ERR_NO_MEMORY = -4,
    CHRONOS_ERR_INTERNAL = -5
} ChronosResult_t;

/* -------------------------------------------------------------------------
 * HAL lifecycle
 * -------------------------------------------------------------------------
 *
 * Example:
 *
 *   hal_init();
 *   ...
 *   hal_shutdown();
 *
 * On a PC this may initialize timers or sockets.
 * On an embedded board it could initialize clocks or peripherals.
 */
ChronosResult_t hal_init(void);
ChronosResult_t hal_shutdown(void);

/* -------------------------------------------------------------------------
 * Timing functions
 * -------------------------------------------------------------------------
 *
 * Timing is extremely important in RAFT.
 *
 * Leader election depends on:
 *   - heartbeat intervals
 *   - election timeouts
 *   - retry timing
 *
 * If timing becomes inconsistent, the cluster may:
 *   - trigger unnecessary elections,
 *   - oscillate between leaders,
 *   - or become unstable.
 *
 * That is why the timing layer is abstracted cleanly here.
 */

/*
 * Return current time in milliseconds.
 *
 * Example:
 *   uint64_t now = hal_get_time_ms();
 */
uint64_t hal_get_time_ms(void);

/*
 * Return current time in microseconds.
 *
 * Useful for:
 *   - latency measurement,
 *   - profiling,
 *   - packet timing experiments.
 */
uint64_t hal_get_time_us(void);

/*
 * Delay execution for a number of milliseconds.
 *
 * Example:
 *   hal_delay_ms(10);
 *
 * In the paper repo we use small delays so the main loop:
 *   - does not consume 100% CPU,
 *   - behaves more predictably,
 *   - produces cleaner timing logs.
 */
void hal_delay_ms(uint32_t delay_ms);

/* -------------------------------------------------------------------------
 * Logging / console output
 * -------------------------------------------------------------------------
 *
 * Instead of scattering printf() everywhere,
 * we centralize logging through HAL.
 *
 * Why this is useful:
 *   - easier to redirect logs later,
 *   - easier to timestamp logs,
 *   - easier to disable logs on embedded targets,
 *   - cleaner experiment output.
 *
 * Example:
 *   hal_console_log("[RAFT] Node became leader");
 */
void hal_console_log(const char* fmt, ...);

/* -------------------------------------------------------------------------
 * Utility helper
 * -------------------------------------------------------------------------
 *
 * Sometimes experiments need a random timeout offset.
 *
 * Example:
 *   RAFT election timeout randomization
 *
 * Without randomization:
 *   every node might start elections at the same time,
 *   causing repeated split votes.
 *
 * Small random jitter improves cluster stability.
 */
uint32_t hal_random_u32(void);

#endif /* CHRONOS_HAL_H */