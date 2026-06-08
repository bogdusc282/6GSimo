# 6GSimo 1.5

**6GSimo 1.5** is a **packet-level discrete-event 6G core network simulator** for research and teaching. It models a simplified **3GPP-style** separation of control and user plane—UE, RAN (gNB), AMF, SMF, PCF, UPF, and a data network.

Install as the Python package **`sixg-sim`** (`import sixg_sim`). Release **1.5.0** (see `pyproject.toml`).

This release includes **two ready-made scenarios** (`demo_simple.py`, `demo_complex.py`) that map directly onto the simulator’s features—use them to learn the tool, then copy and extend them for your own work.

---

## How the simulator and the demos fit together

6GSimo has two layers:

1. **The engine** (`sixg_sim/`) — discrete-event scheduler, network functions, control-plane module, observability. This is the same for every run.
2. **A scenario** (a `.py` file) — chooses topology, traffic, queue limits, and which optional engine features are turned on for that run.

When you run `python -m sixg_sim --scenario path/to/scenario.py`, the CLI loads the scenario, calls `build_simulation()`, advances simulated time until `--until`, then writes **`summary.json`** and **`packet_lifecycle.csv`** next to the scenario file.

```
  ┌─────────────────────────────────────────────────────────┐
  │  sixg_sim engine (always available)                      │
  │  · Event queue, Packet, Link, Entity queueing            │
  │  · UE / RAN / AMF / SMF / PCF / UPF / DN                   │
  │  · Control plane modes 0–3, overload & event-driven hooks  │
  └───────────────────────────┬─────────────────────────────┘
                              │ build_simulation() or SCENARIO
              ┌───────────────┴───────────────┐
              ▼                               ▼
     demo_simple.py                   demo_complex.py
     (minimal wiring)                 (ModularScenarioSpec)
```

### Feature map: engine capability → which demo uses it

| Simulator capability | What it does in the model | `demo_simple` | `demo_complex` |
|----------------------|---------------------------|:-------------:|:--------------:|
| [Discrete-event queueing](#discrete-event-engine) | Per-NF FIFO + server; delay from contention | ✓ | ✓ |
| [Control-plane signalling packets](#control-plane-signalling) | NAS/PDU messages as `Packet` on AMF/SMF/PCF path | ✓ | ✓ |
| [Baseline CP (mode 0)](#control-plane-decision-module) | ~10 ms decisions at CP server | ✓ (default) | ✓ (default) |
| [AI / hybrid CP (modes 1–3)](#control-plane-decision-module) | Longer or adaptive CP service | — | ✓ via env |
| [Periodic uplink traffic](#user-plane-traffic) | Default UE period (~100 pps class) | ✓ | ✓ (URLLC + flow) |
| [Poisson / URLLC traffic](#user-plane-traffic) | Stochastic inter-arrivals | — | ✓ |
| [Custom topology](#topology-and-forwarding) | Arbitrary nodes and link delays | star only | ✓ tiered UPF |
| [Finite NF queues](#finite-queues-and-drops) | Bounded ingress, drop counter | default caps | ✓ tuned caps |
| [UPF background load](#upf-background-and-priority) | Synthetic load in priority queue | — | ✓ |
| [UPF dual QoS queues](#upf-background-and-priority) | Priority vs best-effort at UPF | — | ✓ |
| [UPF overload → CP](#upf-overload-and-cp-reconfigure) | Degrade UPF; CP `RECONFIGURE_UPF` | — | ✓ |
| [Event-driven CP](#event-driven-control-plane) | Extra CP when RAN/UPF backlog high | — | ✓ via env |
| [DN / UE hairpin](#user-plane-traffic) | UE-to-UE traffic via core | — | ✓ |
| [Packet lifecycle trace](#observability) | Per-hop CSV | ✓ | ✓ |
| [UPF episode metrics](#observability) | Degradation integrals in `summary.json` | — | ✓ |
| [M/M/1 validation mode](#optional-mm1-validation) | Reduced path for theory comparison | — | — (env only) |

**Reading the table:** start with **`demo_simple`** to see queueing, signalling, baseline CP, and traces. Use **`demo_complex`** to exercise load, overload, advanced UPF behaviour, and optional CP modes.

---

## Simulator capabilities (detailed)

These sections describe what the **engine** implements. The demos only **configure** a subset; the map above shows where.

### Discrete-event engine

Simulated time jumps from event to event (packet arrival, end of link delay, end of service at a network function, CP completion timer, etc.). There is no fixed Δt time step.

Each network function is an **`Entity`** with:

- One **FIFO ingress queue** (optional capacity limit → drops).
- One **non-preemptive server** (`node_service_s` seconds of work per packet, unless the NF overrides service time, e.g. UPF under degradation).

**Links** add propagation delay only. Contention appears at entity queues, so metrics like `avg_upf_latency_s` in `summary.json` reflect simulated queueing—not a fixed delay formula.

**Seen in demos:** every packet in both demos is scheduled this way. In `summary.json`, see `nodes` for per-NF queue depth stats and `avg_upf_latency_s` for user-plane UPF delay.

---

### Network functions

| Entity | Role | Typical traffic |
|--------|------|-----------------|
| **UE** | Registers with AMF; opens PDU sessions; generates user traffic | User + control (registration) |
| **RAN (gNB)** | Access stratum relay toward AMF (control) and UPF (user) | Both planes |
| **AMF** | Access management; may invoke **CP decision module** for selected signalling | Control |
| **SMF** | Sessions; installs UPF forwarding rules | Control + rule-install to UPF |
| **PCF** | Policy response to SMF | Control |
| **UPF** | User-plane anchor; optional background and strict-priority queues | User (+ some control for rules) |
| **DN** | Sink for user traffic | User |

**Seen in demos:** `demo_simple` uses one of each (star). `demo_complex` uses four RANs, three UPFs (two edge + one core), four UEs.

---

### Control-plane signalling

Before user data flows at scale, the UE sends **control-plane `Packet`s** (e.g. `NAS_REGISTRATION_REQUEST`, `PDU_SESSION_ESTABLISHMENT_REQUEST`). These traverse UE → RAN → AMF → SMF → PCF like signalling in a 5GC teaching model, and each hop uses the same entity queueing as user traffic at that NF.

When AMF handles certain messages, it may call the **CP decision module** (see below). That CP work is **not** a user PDU—it is a separate server with its own wait/service times, recorded in `summary.json` under `control_plane`.

**Seen in demos:** both demos run registration/session setup at the start of the run (typically **3 CP decisions** in a short `demo_simple` run). User traffic dominates the rest of the timeline.

---

### Control-plane decision module

Separate from forwarding packets, the engine hosts a **CP server** (baseline or AI) that processes `CPRequest`s and returns `CPDecision`s (admit, forward to SMF, optional NF actions such as `RECONFIGURE_UPF`).

| `CONTROL_PLANE_MODE` | Name | Nominal behaviour |
|----------------------|------|-------------------|
| `0` | Baseline | ~10 ms per decision stage (deterministic by default) |
| `1` | AI single-stage | ~500 ms nominal AI stage |
| `2` | AI two-stage | Planning + execution stages |
| `3` | Hybrid | Baseline under congestion stress; AI when calm (see `control_plane.py`) |

Environment variables (apply to **any** scenario, especially `demo_complex`):

- `CONTROL_PLANE_AI_LATENCY_DIST` — `deterministic` or `exponential` for AI stages.
- `CP_TOKENS_INPUT`, `CP_TOKENS_OUTPUT`, `CP_PRICE_PER_1M_*` — token and cost accounting in `summary.json`.

**Seen in demos:** `demo_simple` uses mode **0** unless you set env. `demo_complex` is intended for experiments with modes **1–3** (see [How to run](#scenario-democomplexpy-full-feature-showcase) under `demo_complex`).

---

### User-plane traffic

| Mechanism | Description |
|-----------|-------------|
| **Periodic** | Default inter-arrival from `traffic_period_s` on UE |
| **Poisson** | Exponential inter-arrivals (`traffic_flows` or `ue_poisson_arrival_rate`) |
| **URLLC** | Marked UEs use `urlcc_rate_pps` Poisson sources (`ue_urllc_by_index`) |
| **Hairpin** | Destination another UE id; DN reflects back through UPF path (`dn_hairpin_ue_traffic`) |

Override rates with env `TRAFFIC_RATE_PPS` (flows) or `URLLC_RATE_PPS` (URLLC UEs in `demo_complex`).

**Seen in demos:** `demo_simple` — periodic uplink to DN. `demo_complex` — URLLC on UE1–2, Poisson hairpin UE3→UE4, plus default behaviour on other UEs.

---

### Topology and forwarding

- **`demo_simple`** uses `ScenarioConfig` + `build_scenario()` (`scenario.py`): classic star, one UPF, default link profile (UE–RAN jitter optional).
- **`demo_complex`** uses `ModularScenarioSpec` (`modular.py`): explicit nodes, links, and `smf_rule_plan_fn` installing rules on **UPF_EDGE_*** and **UPF_CORE** so uplink visits edge then core (2 ms + 10 ms link delays on user path).

```
demo_simple (star)                 demo_complex (tiered user plane)

     UE1                                UE1 ── gNB_1 ──┐
      │                                 UE2 ── gNB_2 ──┼── UPF_EDGE_1 ── UPF_CORE ── DN
     RAN                                UE3 ── gNB_3 ──┼── UPF_EDGE_2 ──┘
      │                                 UE4 ── gNB_4 ──┘
  AMF SMF PCF                           AMF ── SMF ── PCF (control mesh)
      │                                 (control links to all gNBs / UPFs)
     UPF
      │
     DN
```

---

### Finite queues and drops

Each entity may set `queue_capacity`. When ingress is full, packets are dropped (`drop_count` in `summary.json` → `nodes`).

**Seen in demos:** `demo_simple` uses library defaults (e.g. UE 10, RAN 20, UPF 50). `demo_complex` sets UE 20, each gNB 40, edge UPF 80, core UPF 120—so overload and backlog can build under combined URLLC, hairpin, and background load.

---

### UPF background and priority

The UPF can run **synthetic background** work (`upf_background_mode`, `upf_background_capacity_pct`) in the **priority** queue. With `upf_dual_user_qos_queues`, marked user PDUs may also use priority; other user and control traffic use **best-effort**. One server serves priority head-of-line first (non-preemptive).

**Seen in demos:** only **`demo_complex`** (`upf_background_capacity_pct=25`, `upf_dual_user_qos_queues=True`).

---

### UPF overload and CP reconfigure

If UPF **waiting depth** exceeds `upf_overload_threshold`, the engine (`s2_event_driven.overload_maybe_trigger_cp`) may:

1. Put the UPF in **degraded** mode (2× service time).
2. Submit a CP request (`CP_UPF_QUEUE_OVERLOAD`).
3. On decision with `RECONFIGURE_UPF`, restore nominal UPF service.

Minimum spacing between overload CP events: `upf_overload_cp_min_gap_s` (15 s in `demo_complex`).

**Seen in demos:** only **`demo_complex`** (threshold 8 packets waiting). Watch `control_plane.cp_decision_count` and console **S3_super episode metrics** after a long run.

---

### Event-driven control plane

With `S2_EVENT_DRIVEN_CP=1`, after each simulated event the engine may inject an extra CP request if **any** RAN backlog ≥ `S2_GNB_QUEUE_DEPTH_TRIGGER` (default 7) or **any** UPF backlog ≥ `S2_UPF_QUEUE_THRESHOLD` (default 200), debounced by `S2_MIN_INTER_DECISION_S` (default 30 s).

**Seen in demos:** not enabled by default; enable on **`demo_complex`** for reactive CP under congestion (orthogonal to overload threshold above).

---

### Observability

| Output | Content |
|--------|---------|
| **Console** | Sim time, DN packets received, CP summary line |
| **`summary.json`** | `scenario_py`, `until_s`, `sim_time_s`, `control_plane`, `nodes`, UPF latency percentiles; optional `upf_deg_*` fields when episode tracking is on |
| **`packet_lifecycle.csv`** | Rows per trace segment: queue, service, link per packet |

**Seen in demos:** both enable tracing. `demo_complex` sets `track_s3_super_upf_metrics=True` for degradation episode statistics.

---

### Optional: M/M/1 validation

Set `MM1_VALIDATION=1` (or `ModularScenarioSpec.mm1_validation_mode`) to use a reduced signalling path and simplified UPF for queueing-theory experiments. Neither demo enables this by default; add it in your own scenario if needed.

---

## Scenario: `demo_simple.py` (start here)

**File:** `sixg_sim/examples/demo_simple.py`  
**Wiring:** `ScenarioConfig` + `build_scenario()` — minimal Python, good template for small experiments.  
**Default CLI scenario** when `--scenario` is omitted.

### Topology and behaviour

- **1 UE**, **1 RAN**, **1 AMF**, **1 SMF**, **1 PCF**, **1 UPF**, **1 DN**.
- At simulation start: registration and PDU session establishment (control-plane packets + **3 baseline CP decisions** typical).
- Then **periodic uplink user packets** toward the DN for the rest of the horizon.
- **Packet tracing** enabled → useful `packet_lifecycle.csv` for teaching one packet’s path.

### Which simulator features this demo activates

| Feature | How it appears in this run |
|---------|----------------------------|
| Entity queueing | Light load; shallow queues in `summary.json` → `nodes` |
| Control signalling | Early CP decisions; `control_plane.cp_decision_log` with ~10 ms service each |
| Baseline CP only | `control_plane_mode`: `CP_BASELINE` unless you set env |
| User-plane UPF delay | `avg_upf_latency_s`, `upf_latency_sample_count` grow with horizon |
| No overload / no URLLC / no background | Keeps the story easy to read |

### How to run

```bash
pip install -e .
python -m sixg_sim --until 60
# equivalent:
sixg-demo --until 60
python -m sixg_sim --scenario sixg_sim/examples/demo_simple.py --until 60
```

### What to look for in outputs

- **`summary.json`**: `control_plane` (few decisions at t≈0), `nodes` (UE/RAN/UPF queue stats), rising `upf_latency_sample_count` over 60 s.
- **`packet_lifecycle.csv`**: hops UE → RAN → UPF → DN for user packets; separate control hops during setup.

### Extending simple → complex

Copy `demo_simple.py` when you want a **small** custom star (change `num_ues`, `LinkProfile`, or `CONTROL_PLANE_MODE` in the shell). Move to `ModularScenarioSpec` when you need **custom graphs** like `demo_complex`.

---

## Scenario: `demo_complex.py` (full feature showcase)

**File:** `sixg_sim/examples/demo_complex.py`  
**Wiring:** `SCENARIO = ModularScenarioSpec(...)` + `build_modular_simulation()` — declarative lists of nodes, links, traffic, and flags.

### Topology and behaviour

- **4 UEs** on **4 gNBs**; **UPF_EDGE_1** serves gNB_1–2, **UPF_EDGE_2** serves gNB_3–4; **UPF_CORE** anchors toward **DN**.
- **SMF** installs **tiered forwarding** (`tiered_upf_rule_plan`): UL traffic crosses edge → core; DL reverses.
- **UE1–2:** URLLC Poisson (`urlcc_rate_pps`, default 150/s, env `URLLC_RATE_PPS`).
- **UE3 → UE4:** Poisson hairpin flow (`TRAFFIC_RATE_PPS`, default 120/s).
- **UPF:** 25% background capacity, dual QoS queues, overload when **≥ 8** packets waiting, CP reconfigure with **15 s** min gap.
- **Metrics:** `track_s3_super_upf_metrics=True` → extra fields after overload episodes.

### Which simulator features this demo activates

| Feature | Configuration in file | What you should see |
|---------|----------------------|---------------------|
| Custom topology | `nodes` + `links` | Multi-hop user path; higher event count |
| Finite queues | Capacities on UE/RAN/UPF | Non-zero wait under load; possible drops if pushed |
| URLLC | `ue_urllc_by_index={1,2}` | Steady Poisson load on edge UPFs |
| Hairpin | `traffic_flows` + `dn_hairpin_ue_traffic` | Traffic UE3→UE4 through core |
| UPF background + priority | `upf_background_*`, `upf_dual_user_qos_queues` | Priority queue competes with best-effort |
| Overload → CP | `upf_overload_threshold=8` | More `cp_decision_count`; UPF degraded intervals |
| AI / hybrid CP | *not in file* — set env | Longer `avg_cp_latency_s`, token/cost if AI mode |
| Event-driven CP | *not in file* — `S2_EVENT_DRIVEN_CP=1` | Extra decisions when RAN/UPF backlog high |
| Episode metrics | `track_s3_super_upf_metrics=True` | Console line `S3_super episode metrics: ...` |

### How to run

```bash
# Default complex run (baseline CP, overload path active)
python -m sixg_sim --scenario sixg_sim/examples/demo_complex.py --until 300

# AI single-stage CP + reactive CP on backlog
CONTROL_PLANE_MODE=1 S2_EVENT_DRIVEN_CP=1 \
  python -m sixg_sim --scenario sixg_sim/examples/demo_complex.py --until 300

# Hybrid CP (mode 3)
CONTROL_PLANE_MODE=3 \
  python -m sixg_sim --scenario sixg_sim/examples/demo_complex.py --until 300

# Exponential AI service times
CONTROL_PLANE_MODE=1 CONTROL_PLANE_AI_LATENCY_DIST=exponential \
  python -m sixg_sim --scenario sixg_sim/examples/demo_complex.py --until 300
```

### Environment variables (complex demo)

| Variable | Default in demo | Effect |
|----------|-----------------|--------|
| `TRAFFIC_RATE_PPS` | 120 (hairpin flow) | Poisson rate UE3 → UE4 |
| `URLLC_RATE_PPS` | 150 | URLLC Poisson on UE1–2 |
| `CONTROL_PLANE_MODE` | 0 | Set 1, 2, or 3 for AI/hybrid |
| `CONTROL_PLANE_AI_LATENCY_DIST` | deterministic | `exponential` for random AI service |
| `S2_EVENT_DRIVEN_CP` | off | `1` to enable backlog-triggered CP |
| `S2_GNB_QUEUE_DEPTH_TRIGGER` | 7 | RAN depth to trigger S2 |
| `S2_UPF_QUEUE_THRESHOLD` | 200 | UPF backlog to trigger S2 |
| `S2_MIN_INTER_DECISION_S` | 30 | Debounce between S2 CP events |

### What to look for in outputs

- **`summary.json`**: higher `cp_decision_count` than simple; non-zero `avg_cp_waiting_s` under stress; `upf_deg_episodes_completed` if overload fired; compare runs with `CONTROL_PLANE_MODE=0` vs `1`.
- **`packet_lifecycle.csv`**: longer paths (edge + core UPF); many rows under heavy load (file can be large—shorten `--until` for trials).

---

## Requirements

- **Python 3.10+**
- **NumPy**

---

## Quick start

```bash
cd /path/to/6GSimo-1.5
pip install -e .
sixg-demo
```

Runs **`demo_simple`** for **300 s**, shows welcome banner and live animation (interactive terminal), writes outputs under `sixg_sim/examples/`.

---

## Reading the output files

After each run, 6GSimo prints a short report on the terminal and writes two files **in the same folder as the scenario `.py` file** (for the default run: `sixg_sim/examples/`). This section explains how to read them and how they connect to the [simulator capabilities](#simulator-capabilities-detailed) above.

### Where files land

| Output | Typical path (default run) |
|--------|---------------------------|
| `summary.json` | `sixg_sim/examples/summary.json` |
| `packet_lifecycle.csv` | `sixg_sim/examples/packet_lifecycle.csv` |

If you pass `--scenario path/to/other.py`, both files are written **next to that scenario**, not necessarily under `examples/`.

---

### Terminal lines (right after the run)

Example (numbers will differ):

```text
Simulation finished at t=60.000543s, events remaining=2, DN packets=491
Control plane summary: mode=CP_BASELINE, decisions=3, ai_calls=0, ...
Packet lifecycle CSV written to .../packet_lifecycle.csv
Summary JSON written to .../summary.json
```

| Line | Meaning |
|------|---------|
| **`Simulation finished at t=...`** | Simulated clock when the run stopped (`--until` or empty event queue). |
| **`events remaining=...`** | Events still scheduled but not processed (often a few timers; usually small). |
| **`DN packets=...`** | User packets delivered to the **data network** — main count of successful user-plane throughput. |
| **`Control plane summary`** | Aggregated **CP decision module** stats (not user PDU count). See [`control_plane` in JSON](#control_plane-block-in-summaryjson) below. |
| **Paths at the end** | Exact locations of the two output files. |

Optional, only after **`demo_complex`** with overload tracking:

```text
S3_super episode metrics: completed_episodes=1, wall_deg_s=..., backlog_integral_pkts_s=..., ...
```

That mirrors the extra UPF degradation fields inside `summary.json` ([UPF episode metrics](#observability)).

---

### `summary.json` — structured run report

Open this file in any text editor or JSON viewer. Top-level fields:

| Field | Meaning |
|-------|---------|
| `scenario_py` | Which scenario was run (absolute path). |
| `until_s` | Horizon you requested with `--until`. |
| `sim_time_s` | Actual simulated time at end (may differ slightly from `until_s`). |

#### `control_plane` block in `summary.json`

This is the **[control-plane decision module](#control-plane-decision-module)** only (optimizer queue), not user-data processing at the UPF.

| Field | Meaning |
|-------|---------|
| `control_plane_mode` | e.g. `CP_BASELINE`, `CP_AI_SINGLE` — matches `CONTROL_PLANE_MODE`. |
| `cp_decision_count` | How many CP decisions completed in the run. |
| `cp_total_waiting_time_s` | Sum of queue wait at the CP server across all decisions. |
| `cp_total_service_time_s` | Sum of CP service time across all decisions. |
| `cp_total_latency_time_s` | Sum of end-to-end CP latency per decision (wait + service). |
| `avg_cp_waiting_s`, `avg_cp_service_s`, `avg_cp_latency_s` | Totals divided by `cp_decision_count`. |
| `ai_calls_total`, `tokens_used_total`, `llm_cost_total` | Non-zero when using AI or hybrid CP modes. |
| `cp_decision_log` | List of per-decision records (see below). |

Each entry in **`cp_decision_log`**:

| Field | Meaning |
|-------|---------|
| `t_arrival_s` | Simulated time when that CP request arrived. |
| `wait_time_s` | Time waiting for the CP server to become free. |
| `service_time_s` | Time spent in CP processing for that decision. |
| `latency_time_s` | `wait_time_s` + `service_time_s` for that decision. |
| `ai_calls` | AI stages used (0 for baseline). |

**How to interpret a light run (`demo_simple`, 60 s):** you often see **3 decisions** near `t ≈ 0` (registration/session), then **`cp_decision_count` stays at 3** while **`DN packets`** in the console grows — most of the run is user traffic, not CP.

**How to interpret a heavy run (`demo_complex`):** `cp_decision_count` can be much larger (overload CP, and more if you set `S2_EVENT_DRIVEN_CP=1`). Compare `avg_cp_waiting_s` between `CONTROL_PLANE_MODE=0` and `1` to see AI-induced backlog at the CP server.

#### `nodes` block in `summary.json`

One entry per network function (e.g. `UE1`, `gNB_1`, `UPF_CORE`, `DN`). This reflects **[finite queues](#finite-queues-and-drops)** and sampling of queue depth over time.

| Field | Meaning |
|-------|---------|
| `drop_count` | Packets dropped because ingress queue was full. |
| `queue_capacity` | Max waiting packets (`null` = unlimited for that NF). |
| `max_queue_depth` | Largest backlog seen waiting for service. |
| `avg_queue_depth` | Average waiting depth (sample-based). |
| `time_weighted_avg_queue_depth` | Time-weighted average waiting depth (when capacity is set). |

Low `max_queue_depth` (0–2) under `demo_simple` means little congestion. Higher values or non-zero `drop_count` on `demo_complex` mean load is stressing [queues](#finite-queues-and-drops).

#### User-plane UPF fields (top level)

These summarize **[user-plane delay at the UPF](#discrete-event-engine)** (ingress to end of UPF service), not CP decisions:

| Field | Meaning |
|-------|---------|
| `avg_upf_latency_s` | Mean UPF sojourn time over sampled user packets. |
| `p99_upf_latency_s` | 99th percentile UPF sojourn time. |
| `upf_drop_count` | User/control packets dropped at any UPF. |
| `upf_latency_sample_count` | How many packets contributed a latency sample. |

On a short, light run, values are often microseconds. On a loaded `demo_complex` run, averages rise and `upf_latency_sample_count` grows large.

#### Optional: UPF degradation episode fields

Present when the scenario sets `track_s3_super_upf_metrics=True` (**`demo_complex`**):

| Field | Meaning |
|-------|---------|
| `upf_deg_episodes_completed` | How often overload → reconfigure cycle completed. |
| `upf_deg_wall_time_s` | Total simulated time UPF spent in degraded (slow) mode. |
| `upf_deg_backlog_integral_packets_s` | Integral of backlog during degraded intervals (congestion exposure). |
| `avg_upf_latency_in_cp_deg_episode_s` | Mean UPF latency for packets whose ingress fell inside a degraded episode. |

---

### `packet_lifecycle.csv` — per-hop packet trace

This file is a **CSV table** of trace events. It supports the same [packet lifecycle trace](#observability) described for the engine: one packet can produce many rows as it moves through the core.

**Important:** on long or busy runs the file can become **very large** (the engine caps at 500 000 rows by default and prints a warning if capped). For a first look, use a **short** horizon (e.g. `--until 10`) or open only the first rows.

#### Columns

| Column | Meaning |
|--------|---------|
| `time_s` | Simulated time of this trace event. |
| `trace_id` | ID tying rows to one logical packet (filter on this to follow a single journey). |
| `phase` | Event kind, e.g. `INGRESS`, `NODE_START`, `NODE_END`, `LINK`, `UPF_LATENCY`, `DROP`. |
| `at_entity` | Network function where the event occurred (`UE1`, `gNB_1`, `UPF_CORE`, …). |
| `peer` | Neighbour involved (for links or routing), if any. |
| `plane` | `USER` or `CONTROL`. |
| `summary` | Short label (often message type or event name). |
| `detail` | Extra text (queue depth, drop reason, etc.). |
| `queue_depth` / `queue_capacity` | Snapshot when relevant to [queueing](#finite-queues-and-drops). |
| `latency_upf_s` | Present on `UPF_LATENCY` rows: UPF sojourn for that packet. |

#### How to follow one packet

1. Sort or filter by **`trace_id`** (spreadsheet, `pandas`, or `grep` in the shell).
2. Sort by **`time_s`** within that id.
3. Read the sequence: typically **`INGRESS`** → **`NODE_START`** / **`NODE_END`** at each NF → **`LINK`** delays between nodes.

**Control-plane setup (early in the run):** look for `plane=CONTROL` and summaries like `NAS_REGISTRATION_REQUEST` — path UE → RAN → AMF, related to [control-plane signalling](#control-plane-signalling).

**User traffic (most of the run):** `plane=USER` rows showing UE → RAN → UPF (→ UPF_CORE on `demo_complex`) → DN.

#### Relating CSV to `summary.json`

| Question | Where to look |
|----------|----------------|
| How many packets reached the DN? | Terminal `DN packets=` or infer from volume of USER traces to `DN`. |
| Average UPF delay? | `summary.json` → `avg_upf_latency_s` (aggregated). |
| Why was one packet slow? | `packet_lifecycle.csv` → filter `trace_id`, check `queue_depth` and `UPF_LATENCY`. |
| How busy was the CP optimizer? | `summary.json` → `control_plane` (not the CSV). |
| Were packets dropped? | `summary.json` → `nodes.*.drop_count`, `upf_drop_count`; CSV `phase=DROP`. |

---

### Suggested reading order after your first run

1. Read the **terminal** lines (did the run finish? how many DN packets?).
2. Open **`summary.json`** → `control_plane` (what happened at session start?) → `avg_upf_latency_s` and `nodes` (how loaded was the core?).
3. Optionally open **`packet_lifecycle.csv`** for one `trace_id` on a **short** run to see hops match the [topology diagrams](#topology-and-forwarding) for your scenario.

For feature-specific expectations, see [What to look for in outputs](#what-to-look-for-in-outputs) under each demo scenario.

---

## Command reference

| Command | Meaning |
|---------|---------|
| `sixg-demo` | `python -m sixg_sim` |
| `--scenario PATH` | Scenario module (default: `demo_simple.py`) |
| `--until SECONDS` | Simulated stop time (default `300`) |
| `--packet-log FILE` | Lifecycle CSV beside scenario dir |

| Variable | Effect |
|----------|--------|
| `SIXG_SIM_NO_BANNER=1` | No welcome banner |
| `SIXG_SIM_NO_ANIMATION=1` | No run animation |
| `NO_COLOR=1` | Plain terminal |

---

## Defining your own scenario

| Style | Example | Best for |
|-------|---------|----------|
| Imperative star | `demo_simple.py` → `ScenarioConfig` | Quick changes to UE count and link profile |
| Declarative graph | `demo_complex.py` → `ModularScenarioSpec` | Custom topologies, UPF features, traffic flows |

```bash
python -m sixg_sim --scenario path/to/your_scenario.py --until T
```

Use the [feature map](#feature-map-engine-capability--which-demo-uses-it) as a checklist for which `ModularScenarioSpec` fields and env vars to set.

---

## Package layout

```
sixg_sim/
  __main__.py          # CLI (sixg-demo)
  banner.py            # Welcome art
  run_animation.py     # Live run animation
  core.py              # Packet, Event
  simulation.py        # Simulation, Entity, Link
  entities.py          # Network functions
  control_plane.py     # CP modes, cost model
  modular.py           # ModularScenarioSpec loader
  scenario.py          # Star builder (demo_simple)
  s2_event_driven.py   # Overload + S2 CP hooks
  examples/
    demo_simple.py     # Default scenario
    demo_complex.py    # Full feature showcase
```

---

## License

See **`LICENSE`** (research and academic use; commercial use requires permission).
