# CHRONOS Consensus

**Hardware-timed RAFT evaluation under dynamic wireless conditions**

---

## Overview

CHRONOS is an experimental framework for evaluating **distributed consensus (RAFT)** under **time-varying network conditions**.

Instead of focusing on implementing the consensus algorithm itself, this project investigates how **timing accuracy** and **network variability** affect consensus behavior.

The framework combines:

* A **RISC-V (Milk-V Duo S)** RAFT implementation
* **Cycle-accurate timing** using the RISC-V `rdtime` CSR (assembly)
* A **controlled impairment model** (delay, loss, variability)
* A reproducible **baseline vs. dynamic experiment methodology**

> Goal: Understand how consensus behaves when network assumptions break.

---

## Key Ideas

* **Time is critical**
  Consensus correctness depends on precise timeout behavior.

* **Controlled → Realistic progression**
  Start with controlled impairments → extend to SDR-based real traces.

* **Measure, don’t assume**
  Focus on observable behavior (latency, stability, retries).

* **Reproducibility first**
  Same inputs must produce comparable results.

---

## Features

* RAFT roles: **Follower → Candidate → Leader**
* Hardware-timed execution using **`rdtime`**
* Atomic state updates via **LR/SC**
* Runs on **Milk-V Duo S (RV32)**
* Cross-compiled static binary
* Experiment-ready logging and execution

---

## Repository Structure

```
chronos-consensus/
│
├── core/                # RAFT implementation
├── experiments/         # Logs and results
├── scripts/             # Run + analysis scripts
├── figures/             # Paper figures
└── paper/               # LaTeX draft
```

---

## Build

```bash
make clean
make
```

---

## Run (Milk-V Duo)

```bash
scp raft_test root@<milkv_ip>:/tmp/
ssh root@<milkv_ip>
chmod +x /tmp/raft_test
/tmp/raft_test
```

---

## Experiment Design

We evaluate RAFT under two controlled scenarios:

### 1. Baseline (Control)

* No delay
* No packet loss
* Stable timing conditions

### 2. Impaired (Dynamic)

* Introduced variability (delay / loss)
* Simulated unreliable communication
* Models wireless/network instability

Each experiment is executed multiple times to ensure consistency.

---

## Metrics

We evaluate consensus behavior using the following metrics:

### Election Latency

Time required to transition from follower to leader
→ Measures responsiveness

---

### Election Success Rate

Percentage of successful elections
→ Measures reliability

---

### Election Rounds

Number of retries before leader selection
→ Measures instability

---

### Leader Stability Duration

Time a leader remains active
→ Measures system stability

---

### Timeout Trigger Rate

Frequency of timeout events
→ Measures sensitivity to delay

---

### Latency Variance

Spread of election latency across runs
→ Measures predictability

---

## Data Processing

Experiment outputs are stored as raw logs in:

```
experiments/logs/
```

Processing pipeline:

1. Run baseline and impaired experiments
2. Extract timing and state transitions
3. Compute summary statistics
4. Generate comparison plots

Scripts for processing are located in:

```
scripts/
```

Example:

```bash
python3 scripts/parse_results.py
```

---

## Expected Results

* **Baseline:** fast and stable leader election
* **Impaired:** increased latency and instability

> Key insight:
> Consensus protocols are highly sensitive to timing disruptions and unreliable communication.

---

## Roadmap

* [x] Hardware-timed RAFT (Milk-V)
* [x] Baseline vs. impaired experiments
* [ ] Multi-node communication
* [ ] SDR-based trace integration
* [ ] Scalable simulation (optional)
* [ ] Full paper evaluation

---

## Limitations

* Single-node simulation (votes are local)
* No real network communication yet
* No log replication (leader election only)

---

## License

MIT License

This project is open and free to use, modify, and distribute with attribution.

