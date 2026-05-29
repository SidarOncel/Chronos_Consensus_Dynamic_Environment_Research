#ifndef CHRONOS_HAL_H
#define CHRONOS_HAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>

#include "common_types.h"

/*
 * HAL = Hardware Abstraction Layer
 *
 * This file separates the consensus logic from platform-specific details.
 * RAFT should not care whether it runs on a Windows laptop, Linux PC,
 * Milk-V Duo, or another RISC-V board.
 */

/* HAL lifecycle */
ChronosResult_t hal_init(void);
ChronosResult_t hal_shutdown(void);

/* Timing */
uint64_t hal_get_time_ms(void);
uint64_t hal_get_time_us(void);
void hal_delay_ms(uint32_t delay_ms);

/* Logging */
void hal_console_log(const char* fmt, ...);

/* Utility */
uint32_t hal_random_u32(void);

#endif /* CHRONOS_HAL_H */