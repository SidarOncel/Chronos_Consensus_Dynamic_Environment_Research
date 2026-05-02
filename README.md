# CHRONOS Consensus

**Hardware-timed RAFT evaluation under dynamic wireless conditions**

---

## Overview

CHRONOS is an experimental framework for evaluating **distributed consensus (RAFT)** under **time-varying network conditions**. It combines:

* A **RISC-V (Milk-V Duo S)** implementation of RAFT leader election
* **Cycle-accurate timing** using the RISC-V `rdtime` CSR (assembly)
* A **trace-driven impairment model** (delay/loss/jitter) to emulate wireless dynamics
* A reproducible **baseline vs. impaired** experimental methodology

> Focus: Not re-implementing RAFT, but **measuring how timing and network variability affect consensus behavior**.

---

## Key Ideas

* **Time matters**: Election timeouts and heartbeats depend on precise timing.
* **Controlled → Realistic**: Start with controlled impairments (delay/loss), then extend to **SDR-derived traces**.
* **Reproducibility**: Same code, same settings → comparable runs.
* **Metrics over features**: Election latency and stability are the primary outputs.

---

## Features

* RAFT roles: **Follower → Candidate → Leader**
* Hardware-accurate timing via **`rdtime`**** (assembly)**
* Atomic term update via **LR/SC**
* Cross-compiled **static RISC-V binary**
* Runs on **Milk-V Duo S** (RV32)
* Experiment harness for **baseline vs. impaired** runs

---

## Repository Structure

```
chronos-consensus/
│
├── core/                      # RAFT implementation (C + RISC-V ASM)
│   ├── main.c                # Test harness (Milk-V execution)
│   ├── raft.c                # RAFT state machine
│   ├── raft.h                # Types and API
│   ├── raft_core.S           # rdtime + atomic ops
│   └── Makefile              # Cross-compilation
│
├── experiments/               # Experiment outputs
│   ├── logs/
│   │   ├── baseline.txt
│   │   └── impaired.txt
│   └── results/              # Parsed CSV / summaries
│
├── scripts/                   # Automation
│   ├── run_baseline.sh
│   ├── run_impaired.sh
│   └── parse_results.py
│
├── figures/                   # Paper figures
│   └── plots/
│
└── paper/                     # LaTeX draft
    └── main.tex
```

---

## Requirements

### Hardware

* Milk-V Duo S (RV32)
* Network access (SSH)

### Toolchain (host)

* `riscv64-linux-gnu-gcc`
* `make`

### Target (Milk-V)

* Basic Linux userspace with `sh`, `chmod`, `scp/ssh`

---

## Build

On your host machine:

```bash
make clean
make
```

This produces a static binary: `raft_test`

---

## Deploy & Run

Copy to the board:

```bash
scp raft_test root@<milkv_ip>:/tmp/
ssh root@<milkv_ip>
chmod +x /tmp/raft_test
/tmp/raft_test
```

Example output:

```
Start: role=FOLLOWER term=0 votes=0
...
tick 10: role=LEADER term=1 votes=3
Leader elected at tick 10
```

---

## Instrumentation (important)

To turn runs into **data**, record election latency:

In `main.c`:

```c
uint32_t election_start = 0;
uint32_t election_end = 0;

/* when becoming candidate */
election_start = now;

/* when becoming leader */
election_end = now;
printf("Election latency (ticks): %u\n", election_end - election_start);
```

---

## Experiments

### 1) Baseline (control)

* No artificial delay or loss
* Run multiple times (≥10)
* Output → `experiments/logs/baseline.txt`

```bash
# scripts/run_baseline.sh
for i in {1..10}; do
  /tmp/raft_test >> ../experiments/logs/baseline.txt
done
```

---

### 2) Impaired (controlled variability)

Simulate unreliable communication inside the logic (first step):

* Random vote drops / delays

Example modification in `raft.c` (candidate phase):

```c
if (rand() % 100 < 70) {  // 30% loss
    s->votes++;
}
```

Run:

```bash
# scripts/run_impaired.sh
for i in {1..10}; do
  /tmp/raft_test >> ../experiments/logs/impaired.txt
done
```

> Later, replace this with **trace-driven impairments** (from SDR).

---

## Metrics

Primary:

* **Election latency (ticks)**
* **Number of re-elections / role changes**

Secondary (later):

* Commit latency
* Availability (time with a stable leader)

---

## Data Processing

Parse logs into CSV:

```python
# scripts/parse_results.py (sketch)
import re, sys

def parse(path):
    vals = []
    with open(path) as f:
        for line in f:
            m = re.search(r"Election latency.*: (\d+)", line)
            if m:
                vals.append(int(m.group(1)))
    return vals

base = parse("../experiments/logs/baseline.txt")
imp  = parse("../experiments/logs/impaired.txt")

print("baseline:", base)
print("impaired:", imp)
```

Compute mean/median and plot (matplotlib).

---

## Expected Results (Story)

* **Baseline**: fast, consistent leader election
* **Impaired**: increased latency, possible retries/instability

> Insight: **Consensus is sensitive to timing and network variability**. Controlled impairments reveal causal effects; SDR traces will validate realism.

---

## Roadmap

* [x] Single-node timing-accurate RAFT on Milk-V
* [x] Baseline vs. impaired (synthetic) experiments
* [ ] Multi-process nodes (networked on localhost)
* [ ] Trace-driven impairments (SDR → SNR → loss/delay)
* [ ] Optional ns-3 integration for scale
* [ ] Paper figures and full evaluation

---

## Notes on Assembly

* `rdtime` provides **cycle-accurate timing** on RV32
* `lr.w/sc.w` ensures **atomic term updates**
* Unsigned arithmetic handles **timer wrap-around**

---

## Limitations (current)

* Votes are simulated locally (no real RPC yet)
* No log replication (leader election only)
* Synthetic impairments (SDR integration pending)

---

## Citation (draft)

> CHRONOS: Evaluating Consensus under Dynamic Wireless Conditions using Hardware-Timed RAFT and Trace-Driven Impairments.

---

## License

TBD
