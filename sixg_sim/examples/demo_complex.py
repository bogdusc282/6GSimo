"""6GSimo — complex demo scenario.

Multi-UE topology with edge and core UPFs, finite queues, URLLC Poisson sources,
UPF background load, overload-driven control plane, and optional study hooks via env.

Illustrates:
- Custom topology and SMF rule planning (tiered edge + core UPF)
- Poisson / hairpin user traffic and per-UE URLLC rates
- Strict-priority UPF queues (background vs best-effort user/control)
- UPF overload detection → CP reconfigure path (shared with S3-style engine hooks)
- AI / hybrid control plane when ``CONTROL_PLANE_MODE`` is set (1, 2, or 3)
- Event-driven CP when ``S2_EVENT_DRIVEN_CP=1`` (RAN/UPF backlog triggers)
- Rich ``summary.json`` (UPF latency, per-node queues, optional super episode metrics)

Suggested runs::

    # AI single-stage CP + event-driven triggers
    CONTROL_PLANE_MODE=1 S2_EVENT_DRIVEN_CP=1 \\
        python -m sixg_sim --scenario sixg_sim/examples/demo_complex.py --until 300

    # Hybrid CP (mode 3)
    CONTROL_PLANE_MODE=3 \\
        python -m sixg_sim --scenario sixg_sim/examples/demo_complex.py --until 300

Env knobs (optional): ``TRAFFIC_RATE_PPS``, ``CONTROL_PLANE_AI_LATENCY_DIST``,
``S2_GNB_QUEUE_DEPTH_TRIGGER``, ``S2_UPF_QUEUE_THRESHOLD``, ``S2_MIN_INTER_DECISION_S``.
"""

from __future__ import annotations

import os

from sixg_sim.entities import SMF
from sixg_sim.modular import (
    LinkSpec,
    ModularScenarioSpec,
    NetworkNodeSpec,
    TrafficFlowSpec,
    build_modular_simulation,
)
from sixg_sim.simulation import Simulation

_MS2S = 1e-3
_MU_PPS = 800.0
_NODE_SERVICE_S = 1.0 / _MU_PPS


def tiered_upf_rule_plan(smf: SMF, ue_id: str, sid: int, qos: dict) -> list[tuple[str, list[dict]]]:
    """Install forwarding on edge UPF (near RAN) and core UPF (anchor toward DN)."""
    idx = int(str(ue_id).removeprefix("UE"))
    edge = "UPF_EDGE_1" if idx <= 2 else "UPF_EDGE_2"
    ran = f"gNB_{idx}"
    q = dict(qos)
    rules_edge = [
        {"ue_id": ue_id, "session_id": sid, "direction": "UL", "next_hop": "UPF_CORE", "qos": q},
        {"ue_id": ue_id, "session_id": sid, "direction": "DL", "next_hop": ran, "qos": q},
    ]
    rules_core = [
        {"ue_id": ue_id, "session_id": sid, "direction": "UL", "next_hop": smf.dn_id, "qos": q},
        {"ue_id": ue_id, "session_id": sid, "direction": "DL", "next_hop": edge, "qos": q},
    ]
    return [(edge, rules_edge), ("UPF_CORE", rules_core)]


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


SCENARIO = ModularScenarioSpec(
    nodes=(
        NetworkNodeSpec("AMF1", "amf", node_service_s=_NODE_SERVICE_S),
        NetworkNodeSpec(
            "SMF1",
            "smf",
            node_service_s=_NODE_SERVICE_S,
            peer_bindings={
                "upf_id": "UPF_CORE",
                "pcf_id": "PCF1",
                "ran_id": "gNB_1",
                "dn_id": "DN",
            },
        ),
        NetworkNodeSpec("PCF1", "pcf", node_service_s=_NODE_SERVICE_S),
        NetworkNodeSpec("DN", "dn", node_service_s=0.0),
        NetworkNodeSpec("gNB_1", "ran", node_service_s=_NODE_SERVICE_S, queue_capacity=40),
        NetworkNodeSpec("gNB_2", "ran", node_service_s=_NODE_SERVICE_S, queue_capacity=40),
        NetworkNodeSpec("gNB_3", "ran", node_service_s=_NODE_SERVICE_S, queue_capacity=40),
        NetworkNodeSpec("gNB_4", "ran", node_service_s=_NODE_SERVICE_S, queue_capacity=40),
        NetworkNodeSpec(
            "UPF_EDGE_1",
            "upf",
            node_service_s=_NODE_SERVICE_S,
            peer_bindings={"dn_id": "DN"},
            queue_capacity=80,
        ),
        NetworkNodeSpec(
            "UPF_EDGE_2",
            "upf",
            node_service_s=_NODE_SERVICE_S,
            peer_bindings={"dn_id": "DN"},
            queue_capacity=80,
        ),
        NetworkNodeSpec(
            "UPF_CORE",
            "upf",
            node_service_s=_NODE_SERVICE_S,
            peer_bindings={"dn_id": "DN"},
            queue_capacity=120,
        ),
    ),
    links=(
        LinkSpec("AMF1", "gNB_1", 1e-3),
        LinkSpec("AMF1", "gNB_2", 1e-3),
        LinkSpec("AMF1", "gNB_3", 1e-3),
        LinkSpec("AMF1", "gNB_4", 1e-3),
        LinkSpec("AMF1", "SMF1", 1e-3),
        LinkSpec("SMF1", "PCF1", 1e-3),
        LinkSpec("SMF1", "UPF_CORE", 1e-3),
        LinkSpec("SMF1", "UPF_EDGE_1", 1e-3),
        LinkSpec("SMF1", "UPF_EDGE_2", 1e-3),
        LinkSpec("gNB_1", "UPF_EDGE_1", 2 * _MS2S),
        LinkSpec("gNB_2", "UPF_EDGE_1", 2 * _MS2S),
        LinkSpec("gNB_3", "UPF_EDGE_2", 2 * _MS2S),
        LinkSpec("gNB_4", "UPF_EDGE_2", 2 * _MS2S),
        LinkSpec("UPF_EDGE_1", "UPF_CORE", 10 * _MS2S),
        LinkSpec("UPF_EDGE_2", "UPF_CORE", 10 * _MS2S),
        LinkSpec("UPF_CORE", "DN", 0.5 * _MS2S),
    ),
    num_ues=4,
    ue_attachment={
        1: ("gNB_1", "AMF1"),
        2: ("gNB_2", "AMF1"),
        3: ("gNB_3", "AMF1"),
        4: ("gNB_4", "AMF1"),
    },
    ue_queue_capacity=20,
    ue_urllc_by_index={1: True, 2: True},
    urlcc_rate_pps=_env_float("URLLC_RATE_PPS", 150.0),
    traffic_flows=(
        TrafficFlowSpec(
            src_ue_index=3,
            dst="UE4",
            rate_pps=_env_float("TRAFFIC_RATE_PPS", 120.0),
            size_bytes=800,
            arrival_process="poisson",
        ),
    ),
    dn_hairpin_ue_traffic=True,
    smf_rule_plan_fn=tiered_upf_rule_plan,
    upf_background_mode="poisson",
    upf_background_capacity_pct=25.0,
    upf_dual_user_qos_queues=True,
    upf_overload_threshold=8,
    upf_overload_cp_min_gap_s=15.0,
    track_s3_super_upf_metrics=True,
    packet_tracing=True,
    event_logging=True,
)


def build_simulation() -> Simulation:
    return build_modular_simulation(SCENARIO)
