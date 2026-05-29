    .section .text

    /*
     * Export the symbols so the C files can link against them.
     *
     * These are the tiny hardware-facing helpers used by the RAFT core:
     *   - rdtime32(): read the hardware time counter
     *   - check_timeout_ticks(): compare elapsed time against timeout
     *   - atomic_inc_term_u32(): atomically increment the term counter
     *   - raft_majority_reached(): check whether enough votes were collected
     */
    .globl rdtime32
    .globl check_timeout_ticks
    .globl atomic_inc_term_u32
    .globl raft_majority_reached

/********************************************************************
 * uint32_t rdtime32(void)
 *
 * What it does:
 *   Reads the RISC-V time CSR and returns the current tick value.
 *
 * Why this matters:
 *   RAFT depends on timing. If timeouts are inaccurate, the cluster can
 *   start elections too early or too late.
 *
 * Example:
 *   uint32_t now = rdtime32();
 ********************************************************************/
rdtime32:
    rdtime a0          /* Put current time into return register a0 */
    ret                /* Return to C caller */


/********************************************************************
 * int check_timeout_ticks(uint32_t last_tick, uint32_t timeout_ticks)
 *
 * Arguments:
 *   a0 = last_tick
 *   a1 = timeout_ticks
 *
 * Returns:
 *   a0 = 1 if timeout expired
 *   a0 = 0 if still within timeout
 *
 * How it works:
 *   elapsed = now - last_tick
 *   if elapsed >= timeout_ticks -> timeout expired
 *
 * Why unsigned compare:
 *   Time counters can wrap around, so unsigned subtraction is safer.
 *
 * Example:
 *   if (check_timeout_ticks(last_heartbeat, 15000000)) {
 *       start_election();
 *   }
 ********************************************************************/
check_timeout_ticks:
    rdtime t0              /* t0 = current time */
    sub   t1, t0, a0       /* t1 = elapsed time */
    bgeu  t1, a1, timeout_yes   /* if elapsed >= timeout -> yes */

    li    a0, 0            /* timeout not reached */
    ret

timeout_yes:
    li    a0, 1            /* timeout reached */
    ret


/********************************************************************
 * void atomic_inc_term_u32(uint32_t *term)
 *
 * Argument:
 *   a0 = pointer to term counter
 *
 * What it does:
 *   Atomically increments the current term using LR/SC.
 *
 * Why atomic:
 *   In a concurrent or interrupt-driven environment, two writers must
 *   not update the term at the same time.
 *
 * Example:
 *   atomic_inc_term_u32(&state->current_term);
 ********************************************************************/
atomic_inc_term_u32:
retry:
    lr.w  t0, (a0)         /* Load value and reserve address */
    addi  t0, t0, 1        /* t0 = t0 + 1 */
    sc.w  t1, t0, (a0)     /* Try to store; t1 = 0 on success */
    bnez  t1, retry        /* If store failed, try again */
    ret


/********************************************************************
 * int raft_majority_reached(uint32_t votes, uint32_t total_nodes)
 *
 * Arguments:
 *   a0 = votes
 *   a1 = total_nodes
 *
 * Returns:
 *   a0 = 1 if votes >= floor(total_nodes / 2) + 1
 *   a0 = 0 otherwise
 *
 * Why this is in assembly:
 *   It is tiny, hot, and easy to express directly with shifts/branches.
 *
 * Example:
 *   if (raft_majority_reached(votes, total_nodes)) {
 *       become_leader();
 *   }
 ********************************************************************/
raft_majority_reached:
    srli  t0, a1, 1        /* t0 = total_nodes / 2 */
    addi  t0, t0, 1        /* t0 = majority threshold */

    bltu  a0, t0, no_majority   /* if votes < majority -> false */

    li    a0, 1            /* majority reached */
    ret

no_majority:
    li    a0, 0            /* majority not reached */
    ret