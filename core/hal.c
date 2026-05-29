#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "hal.h"

#include <stdio.h>
#include <stdarg.h>

/*
 * Platform selection:
 *
 * The same source should compile on:
 *   - Windows
 *   - Linux
 *   - Milk-V / embedded Linux
 *
 * The exact timing implementation changes by platform,
 * but the rest of the code should not need to know that.
 */
#ifdef _WIN32
    #define WIN32_LEAN_AND_MEAN
    #include <windows.h>
#else
    #include <time.h>
    #include <unistd.h>
    #include <sys/time.h>
#endif

/*
 * HAL module state
 *
 * We keep a start time so that the rest of the system can work with
 * relative timestamps instead of absolute system clock values.
 *
 * That is useful because:
 *   - logs are easier to read,
 *   - latency measurements are easier to interpret,
 *   - runs from different machines are easier to compare.
 */
static uint64_t g_start_time_ms = 0;
static bool g_initialized = false;

#ifdef _WIN32
static LARGE_INTEGER g_perf_freq;
static LARGE_INTEGER g_perf_start;
#endif

/*
 * Very small internal RNG state.
 *
 * RAFT election timeouts should not all be identical.
 * If all nodes time out at the same moment, you get split votes.
 *
 * This helper gives us a simple random value without needing a large
 * external library.
 */
static uint32_t g_rng_state = 0xA341316C;

/* -------------------------------------------------------------------------
 * Private timing helpers
 * -------------------------------------------------------------------------
 *
 * These functions talk to the underlying operating system.
 * The rest of the code should use hal_get_time_ms() / hal_get_time_us()
 * instead of calling these directly.
 */

static uint64_t get_system_time_ms(void)
{
#ifdef _WIN32
    LARGE_INTEGER current;
    QueryPerformanceCounter(&current);
    return (uint64_t)((current.QuadPart * 1000) / g_perf_freq.QuadPart);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000ULL);
#endif
}

static uint64_t get_system_time_us(void)
{
#ifdef _WIN32
    LARGE_INTEGER current;
    QueryPerformanceCounter(&current);
    return (uint64_t)((current.QuadPart * 1000000) / g_perf_freq.QuadPart);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)(ts.tv_nsec / 1000ULL);
#endif
}

/* -------------------------------------------------------------------------
 * Initialization / shutdown
 * -------------------------------------------------------------------------
 *
 * hal_init() prepares the timing base and the RNG state.
 * hal_shutdown() just resets local state.
 *
 * In the paper repo, this is enough.
 * In the full AGV project, additional hardware setup could be added later.
 */

ChronosResult_t hal_init(void)
{
    if (g_initialized) {
        return CHRONOS_OK;
    }

#ifdef _WIN32
    /*
     * Windows uses a performance counter because it is much better for
     * timing measurements than the normal low-resolution clock.
     */
    if (!QueryPerformanceFrequency(&g_perf_freq)) {
        return CHRONOS_ERR_INTERNAL;
    }
    QueryPerformanceCounter(&g_perf_start);
#endif

    /*
     * Record the first observed time as the origin.
     * All later timestamps become "time since program start".
     */
    g_start_time_ms = get_system_time_ms();

    /*
     * Seed the RNG from the current time.
     * If the time is zero for some reason, keep the default non-zero seed.
     */
    uint64_t seed = g_start_time_ms ^ 0x9E3779B97F4A7C15ULL;
    g_rng_state ^= (uint32_t)(seed & 0xFFFFFFFFu);
    if (g_rng_state == 0) {
        g_rng_state = 0xA341316C;
    }

    g_initialized = true;
    return CHRONOS_OK;
}

ChronosResult_t hal_shutdown(void)
{
    /*
     * Nothing heavy to release in this paper version.
     * We just mark the HAL as not initialized.
     */
    g_initialized = false;
    return CHRONOS_OK;
}

/* -------------------------------------------------------------------------
 * Timing API
 * -------------------------------------------------------------------------
 *
 * These functions are the ones the rest of the project should use.
 * They hide platform details and give a consistent time source.
 */

uint64_t hal_get_time_ms(void)
{
    if (!g_initialized) {
        hal_init();
    }

    return get_system_time_ms() - g_start_time_ms;
}

uint64_t hal_get_time_us(void)
{
    if (!g_initialized) {
        hal_init();
    }

    return get_system_time_us() - (g_start_time_ms * 1000ULL);
}

void hal_delay_ms(uint32_t delay_ms)
{
#ifdef _WIN32
    Sleep(delay_ms);
#else
    /*
     * nanosleep is better than a busy loop because it lets the OS
     * schedule other work while we wait.
     *
     * For an experiment harness, this keeps the log cleaner and avoids
     * burning CPU unnecessarily.
     */
    struct timespec ts;
    ts.tv_sec = delay_ms / 1000U;
    ts.tv_nsec = (long)((delay_ms % 1000U) * 1000000UL);
    nanosleep(&ts, NULL);
#endif
}

/* -------------------------------------------------------------------------
 * Logging
 * -------------------------------------------------------------------------
 *
 * The whole point of using a centralized logger is consistency.
 * Every log line gets a timestamp prefix automatically.
 *
 * That makes it much easier to:
 *   - compare runs,
 *   - measure delays,
 *   - trace leader elections,
 *   - and debug message flow.
 *
 * Example:
 *   [120] [RAFT] State changed: FOLLOWER -> CANDIDATE
 */
void hal_console_log(const char* fmt, ...)
{
    printf("[%8llu] ", (unsigned long long)hal_get_time_ms());

    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);

    printf("\n");
    fflush(stdout);
}

/* -------------------------------------------------------------------------
 * Random helper
 * -------------------------------------------------------------------------
 *
 * This is used for small timeout jitter.
 * Random jitter is important in RAFT because it reduces the chance that
 * every follower times out at the same moment.
 *
 * Example:
 *   without jitter: all nodes become candidates together
 *   with jitter: one node usually starts first and becomes leader
 */
uint32_t hal_random_u32(void)
{
    /*
     * Xorshift32:
     * very small, fast, and enough for timeout jitter.
     * This is not cryptographically secure, and it does not need to be.
     */
    uint32_t x = g_rng_state;

    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;

    g_rng_state = (x == 0) ? 0xA341316C : x;
    return g_rng_state;
}