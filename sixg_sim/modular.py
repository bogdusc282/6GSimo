"""Load user-defined topologies from Python scenario modules.

A scenario module may expose either:

- ``SCENARIO``: a :class:`ModularScenarioSpec` built declaratively, or
  ``MODULAR_SCENARIO`` (legacy alias), or
- ``build_simulation() -> Simulation``: arbitrary wiring using the same entities.

Network **nodes** are typed NF endpoints (UPF, AMF/SMF/PCF, RAN, DN).
Undirected **links** carry propagation latency.  UE attachments select which RAN
and AMF each UE uses for registration and PDU sessions.

**Routing prefixes:** signalling matches AMF by ``AMF*`` and access nodes by ``RAN*`` or ``gNB*``.

User-plane packets carry a logical destination id that must match the
data-network entity id and SMF UL rule targets (defaults align if you name your
DN node ``DN``).

``traffic_flows``: entries set source UE traffic fields after UE construction.
Effective ``rate_pps`` is ``TRAFFIC_RATE_PPS`` from the environment when set (non-empty),
otherwise each flow's ``rate_pps``.

**M/M/1 validation** (env ``MM1_VALIDATION=1`` or ``ModularScenarioSpec.mm1_validation_mode``):
skips control-plane signalling when configured, silences non-flow UEs, optional FIFO UPF,
and optional zero UE/gNB queueing; see ``_mm1_resolve_bundle`` in this module.
"""

from __future__ import annotations

import importlib.util
import os
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from sixg_sim.control_plane import ensure_default_control_plane
from sixg_sim.core import UpfQosClass
from sixg_sim.entities import AMF, DataNetwork, PCF, RAN, SMF, UE, UPF
from sixg_sim.scenario import LinkProfile, _connect, sample_ue_ran_link_latency
from sixg_sim.simulation import Simulation

NetworkNodeKind = Literal["upf", "amf", "smf", "pcf", "ran", "dn"]

TrafficNextDelayFn = Callable[[Simulation, UE, int], float]

SmfRulePlanFn = Callable[[SMF, str, int, dict], list[tuple[str, list[dict]]]]

TrafficArrivalProcess = Literal["fixed", "poisson"]


@dataclass
class NetworkNodeSpec:
    """One infrastructure node (not a UE).

    ``peer_bindings`` overrides automatic neighbour inference. Supported keys:

    - AMF: ``smf_id``
    - SMF: ``upf_id``, ``pcf_id``, ``ran_id``, ``dn_id`` (data network entity id for UL rules)
    - RAN: ``amf_id``, ``upf_id``
    - UPF: ``dn_id``

    ``node_service_s``: optional mean service time (seconds) for this NF's M/M/1-style server.
    If omitted, builders use kind-appropriate defaults from :class:`ModularScenarioSpec`
    (e.g. ``ran_node_service_s``, ``upf_user_plane_service_s``, AMF/SMF/PCF intrinsic defaults).

    ``queue_capacity``: optional max number of packets **waiting** (not including service). ``None`` = unlimited.
    """

    node_id: str
    kind: NetworkNodeKind
    peer_bindings: dict[str, str] = field(default_factory=dict)
    node_service_s: float | None = None
    queue_capacity: int | None = None


@dataclass(frozen=True)
class LinkSpec:
    endpoint_a: str
    endpoint_b: str
    latency_s: float


@dataclass(frozen=True)
class TrafficFlowSpec:
    """User-plane traffic from ``UE{src_ue_index}`` toward ``dst``.

    When ``rate_pps`` is set, the source UE uses mean interval ``1/rate_pps`` for fixed arrivals,
    or exponential inter-arrivals at rate ``rate_pps`` when ``arrival_process=\"poisson\"``.
    """

    src_ue_index: int
    dst: str
    rate_pps: float | None = None
    size_bytes: int = 1200
    arrival_process: TrafficArrivalProcess = "fixed"


def _truthy_env(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true", "yes", "on")


def traffic_flow_effective_rate_pps(flow: TrafficFlowSpec) -> float | None:
    """Prefer ``TRAFFIC_RATE_PPS`` env when set; otherwise ``flow.rate_pps``."""
    raw = os.environ.get("TRAFFIC_RATE_PPS")
    if raw is not None and str(raw).strip() != "":
        return float(raw)
    if flow.rate_pps is not None:
        return float(flow.rate_pps)
    return None


@dataclass
class ModularScenarioSpec:
    """Declarative scenario: nodes + edges + UE counts and traffic behaviour.

    ``traffic_flows`` declares optional per-source flows (at most one entry per ``src_ue_index``).
    ``dn_hairpin_ue_traffic`` enables reflecting user PDUs destined to a ``UE*`` id back via the anchor.
    """

    nodes: Sequence[NetworkNodeSpec]
    links: Sequence[LinkSpec]

    num_ues: int = 1
    default_ran_id: str = "RAN1"
    default_amf_id: str = "AMF1"
    ue_attachment: dict[int, tuple[str, str]] = field(default_factory=dict)

    traffic_period_s: float = 0.01
    ue_registration_jitter_s: float = 0.0
    ue_traffic_period_jitter_relative: float = 0.0
    ue_bernoulli_interarrival: bool = False
    ue_bernoulli_p: float = 0.5
    ue_bernoulli_p_by_index: dict[int, float] = field(default_factory=dict)
    ue_poisson_arrival_rate: float | None = None
    ue_upf_qos_class_by_index: dict[int, int] = field(default_factory=dict)
    traffic_next_delay_s: TrafficNextDelayFn | None = None

    ue_ran_base_latency_s: float = 100e-6
    ue_ran_latency_jitter_s: float = 0.0

    smf_send_upf_rule_install_packet: bool = True
    ran_node_service_s: float | None = None
    upf_user_plane_service_s: float | None = None
    upf_dual_user_qos_queues: bool = False
    upf_background_mode: Literal["poisson", "bernoulli_geom"] = "poisson"
    upf_background_capacity_pct: float = 0.0
    upf_background_chunk_service_s: float | None = None
    upf_background_arrival_slot_s: float = 100e-6
    upf_background_arrival_p: float = 0.3
    upf_background_service_slot_s: float = 10e-6
    upf_background_service_geom_p: float = 0.5
    upf_mixed_approx_validation: bool = False

    #: applied to each :class:`~sixg_sim.entities.UE`; ``None`` = unlimited UE ingress FIFO
    ue_queue_capacity: int | None = None

    event_logging: bool = True
    event_log_max_entries: int = 50_000
    packet_tracing: bool | None = None
    packet_lifecycle_max_entries: int = 500_000

    smf_rule_plan_fn: SmfRulePlanFn | None = None
    traffic_flows: Sequence[TrafficFlowSpec] = ()
    dn_hairpin_ue_traffic: bool = False

    #: ``ScenarioConfig``-aligned S3 DES: UPF waiting-depth overload → CP ``RECONFIGURE_UPF``.
    upf_overload_threshold: int | None = None
    upf_overload_cp_min_gap_s: float = 30.0
    #: Episode-conditioned UPF stats + ∫backlog·dt during overload-degraded intervals (see ``s3_super_des_scenario``).
    track_s3_super_upf_metrics: bool = False
    #: If > 0, each AMF schedules :func:`~sixg_sim.entities.AMF.schedule_cp_dummy_triggers` with this
    #: interval until :meth:`~sixg_sim.simulation.Simulation.run` ``until`` (strategic/non-overload CP).
    #: Used by Scenario S4 so Hybrid modes can observe ``CP_DUMMY_HEARTBEAT`` while overload uses a separate path.
    cp_strategic_dummy_interval_s: float | None = None
    ue_urllc_by_index: dict[int, bool] = field(default_factory=dict)
    urlcc_rate_pps: float = 0.0

    # --- M/M/1 tandem validation (enable with mm1_validation_mode or env MM1_VALIDATION=1) ---
    mm1_validation_mode: bool = False
    #: When validation is active and this is None: skip NAS/PDU/SMF signalling; bootstrap PDU + UPF rules.
    mm1_skip_control_plane: bool | None = None
    #: When validation is active and None: zero UE + RAN node_service unless MM1_INCLUDE_UE_RAN_QUEUEING=1.
    mm1_disable_ue_ran_queueing: bool | None = None
    #: When validation is active and None: False (single FIFO UPF). When inactive: True (strict priority).
    mm1_upf_strict_priority: bool | None = None
    #: When validation is active and None: only traffic_flows sources emit user-plane traffic.
    mm1_only_traffic_flow_sources_emit: bool | None = None
    #: Optional uniform UE ingress/processing mean service time (s); exponential draws).
    ue_user_plane_node_service_s: float | None = None

    def validate(self) -> None:
        if self.num_ues < 1:
            raise ValueError("num_ues must be >= 1")
        ids = [n.node_id for n in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate node_id in nodes")
        node_set = set(ids)
        kinds = {n.node_id: n.kind for n in self.nodes}
        for L in self.links:
            if L.endpoint_a not in node_set or L.endpoint_b not in node_set:
                raise ValueError(f"link references unknown node: {L.endpoint_a} — {L.endpoint_b}")

        upfs = [n.node_id for n in self.nodes if n.kind == "upf"]
        if not upfs:
            raise ValueError("at least one UPF node (kind 'upf') is required")
        dns = [n.node_id for n in self.nodes if n.kind == "dn"]
        if len(dns) != 1:
            raise ValueError("exactly one data network node (kind 'dn') is required")

        for idx in self.ue_attachment:
            if idx < 1 or idx > self.num_ues:
                raise ValueError(f"ue_attachment key {idx} out of range")

        for n in self.nodes:
            if n.queue_capacity is not None and (
                not isinstance(n.queue_capacity, int) or n.queue_capacity < 1
            ):
                raise ValueError(f"node {n.node_id!r}: queue_capacity must be None or an integer >= 1")
        if self.ue_queue_capacity is not None and (
            not isinstance(self.ue_queue_capacity, int) or self.ue_queue_capacity < 1
        ):
            raise ValueError("ue_queue_capacity must be None or an integer >= 1")

        # Minimal sanity aligned with ScenarioConfig
        if self.upf_background_mode not in ("poisson", "bernoulli_geom"):
            raise ValueError("upf_background_mode must be 'poisson' or 'bernoulli_geom'")
        if self.ue_bernoulli_interarrival and not 0.0 < self.ue_bernoulli_p <= 1.0:
            raise ValueError("ue_bernoulli_p must be in (0, 1] when ue_bernoulli_interarrival is True")
        if self.ue_poisson_arrival_rate is not None:
            if self.ue_poisson_arrival_rate <= 0.0:
                raise ValueError("ue_poisson_arrival_rate must be > 0 when set")
            if self.ue_bernoulli_interarrival:
                raise ValueError("ue_poisson_arrival_rate cannot be combined with ue_bernoulli_interarrival")
            if self.traffic_next_delay_s is not None:
                raise ValueError("traffic_next_delay_s cannot be combined with ue_poisson_arrival_rate")
        if self.traffic_next_delay_s is not None:
            if self.ue_bernoulli_interarrival:
                raise ValueError("traffic_next_delay_s cannot be combined with ue_bernoulli_interarrival")

        for nid, k in kinds.items():
            if k == "amf" and not nid.startswith("AMF"):
                raise ValueError(f"AMF node id must start with 'AMF' (got {nid!r}) — required by signalling code")
            if k == "ran" and not (nid.startswith("RAN") or nid.startswith("gNB")):
                raise ValueError(
                    f"RAN / gNB node id must start with 'RAN' or 'gNB' (got {nid!r}) — required by signalling code"
                )

        if self.ue_ran_base_latency_s < 0:
            raise ValueError("ue_ran_base_latency_s must be >= 0")
        if self.ue_ran_latency_jitter_s < 0:
            raise ValueError("ue_ran_latency_jitter_s must be >= 0")

        if self.urlcc_rate_pps < 0.0:
            raise ValueError("urlcc_rate_pps must be >= 0")
        for idx in self.ue_urllc_by_index:
            if idx < 1 or idx > self.num_ues:
                raise ValueError(f"ue_urllc_by_index key {idx} out of range for num_ues={self.num_ues}")
        if self.upf_overload_threshold is not None and int(self.upf_overload_threshold) < 1:
            raise ValueError("upf_overload_threshold must be None or an integer >= 1")
        if self.upf_overload_cp_min_gap_s < 0.0:
            raise ValueError("upf_overload_cp_min_gap_s must be >= 0")

        if self.cp_strategic_dummy_interval_s is not None and float(self.cp_strategic_dummy_interval_s) < 0.0:
            raise ValueError("cp_strategic_dummy_interval_s must be None or >= 0")

        dn_nid = dns[0]
        if self.traffic_flows:
            seen_src: set[int] = set()
            for flow in self.traffic_flows:
                if flow.src_ue_index in seen_src:
                    raise ValueError(f"traffic_flows: duplicate src_ue_index {flow.src_ue_index}")
                seen_src.add(flow.src_ue_index)
                if flow.src_ue_index < 1 or flow.src_ue_index > self.num_ues:
                    raise ValueError(f"traffic_flows: src_ue_index {flow.src_ue_index} out of range")
                if flow.size_bytes < 1:
                    raise ValueError("traffic_flows: size_bytes must be >= 1")
                if flow.rate_pps is not None and flow.rate_pps <= 0.0:
                    raise ValueError("traffic_flows: rate_pps must be > 0 when set")
                raw_env = os.environ.get("TRAFFIC_RATE_PPS")
                env_rate_ok = (
                    raw_env is not None
                    and str(raw_env).strip() != ""
                    and float(raw_env) > 0.0
                )
                if (
                    flow.arrival_process == "poisson"
                    and flow.rate_pps is None
                    and not env_rate_ok
                ):
                    raise ValueError(
                        "traffic_flows: arrival_process='poisson' requires rate_pps "
                        "or positive TRAFFIC_RATE_PPS"
                    )
                d = flow.dst
                if d != dn_nid:
                    if not (isinstance(d, str) and d.startswith("UE")):
                        raise ValueError(f"traffic_flows: unknown dst {d!r} (expected {dn_nid!r} or UE<id>)")
                    rest = d.removeprefix("UE")
                    if not rest.isdigit():
                        raise ValueError(f"traffic_flows: invalid UE dst {d!r}")
                    ui = int(rest)
                    if ui < 1 or ui > self.num_ues:
                        raise ValueError(f"traffic_flows: dst {d!r} out of UE range")
            if any(f.dst.startswith("UE") for f in self.traffic_flows) and not self.dn_hairpin_ue_traffic:
                raise ValueError("traffic_flows targeting a UE require dn_hairpin_ue_traffic=True")

        eff_mm1 = self.mm1_validation_mode or _truthy_env("MM1_VALIDATION")
        only_flow = (
            self.mm1_only_traffic_flow_sources_emit
            if self.mm1_only_traffic_flow_sources_emit is not None
            else True
        )
        if eff_mm1 and only_flow and not self.traffic_flows:
            raise ValueError(
                "mm1_validation_mode (or MM1_VALIDATION): need non-empty traffic_flows when "
                "restricting emitters to flow sources"
            )


def _adjacency(links: Sequence[LinkSpec]) -> dict[str, set[str]]:
    g: dict[str, set[str]] = defaultdict(set)
    for L in links:
        g[L.endpoint_a].add(L.endpoint_b)
        g[L.endpoint_b].add(L.endpoint_a)
    return dict(g)


def _infer_peer(
    adj: Mapping[str, set[str]],
    kinds: Mapping[str, NetworkNodeKind],
    node_id: str,
    want_kind: NetworkNodeKind,
    label: str,
) -> str:
    neigh = sorted(n for n in adj.get(node_id, ()) if kinds.get(n) == want_kind)
    if len(neigh) != 1:
        raise ValueError(
            f"Node {node_id!r}: need exactly one {want_kind!r} neighbour for {label}, found {neigh!r}. "
            f"Set peer_bindings on this node to disambiguate."
        )
    return neigh[0]


def _singleton_kind(kinds: Mapping[str, NetworkNodeKind], kind: NetworkNodeKind, purpose: str) -> str:
    cands = sorted(n for n, k in kinds.items() if k == kind)
    if len(cands) != 1:
        raise ValueError(
            f"Expected exactly one {kind!r} node for {purpose}, found {cands!r}. "
            f"Add peer_bindings or fix topology."
        )
    return cands[0]


def _infer_smf_ran_id(nspec: NetworkNodeSpec, adj: Mapping[str, set[str]], kinds: Mapping[str, NetworkNodeKind]) -> str:
    """SMF often has no direct link to RAN (RAN reaches SMF via AMF); infer ran_id sensibly."""
    nid = nspec.node_id
    neigh_ran = sorted(n for n in adj.get(nid, ()) if kinds.get(n) == "ran")
    if len(neigh_ran) == 1:
        return neigh_ran[0]
    if not neigh_ran:
        return _singleton_kind(kinds, "ran", "SMF ran_id (no SMF–RAN edge)")
    raise ValueError(
        f"Node {nid!r}: multiple RAN neighbours {neigh_ran!r} for SMF ran_id; set peer_bindings['ran_id']"
    )


def _pb(spec: NetworkNodeSpec, key: str, inferred: str) -> str:
    return spec.peer_bindings.get(key, inferred)


def _infer_uniform_user_plane_service_s(spec: ModularScenarioSpec) -> float | None:
    """First positive ``node_service_s`` from RAN/UPF node specs (scenario MU alignment)."""
    for n in spec.nodes:
        if n.kind in ("ran", "upf") and n.node_service_s is not None:
            v = float(n.node_service_s)
            if v > 0.0:
                return v
    if spec.upf_user_plane_service_s is not None and float(spec.upf_user_plane_service_s) > 0.0:
        return float(spec.upf_user_plane_service_s)
    if spec.ran_node_service_s is not None and float(spec.ran_node_service_s) > 0.0:
        return float(spec.ran_node_service_s)
    return None


def _mm1_resolve_bundle(spec: ModularScenarioSpec) -> tuple[bool, bool, bool, bool, bool]:
    """Resolve M/M/1 validation toggles (spec + env MM1_VALIDATION).

    Returns:
        eff_mm1, skip_cp, dis_ue_ran, upf_strict_priority, only_flow_sources_emit
    """
    eff_mm1 = spec.mm1_validation_mode or _truthy_env("MM1_VALIDATION")
    if not eff_mm1:
        upf_strict = True if spec.mm1_upf_strict_priority is None else bool(spec.mm1_upf_strict_priority)
        return False, False, False, upf_strict, False
    skip_cp = True if spec.mm1_skip_control_plane is None else bool(spec.mm1_skip_control_plane)
    if spec.mm1_disable_ue_ran_queueing is not None:
        dis_ue_ran = bool(spec.mm1_disable_ue_ran_queueing)
    else:
        dis_ue_ran = not _truthy_env("MM1_INCLUDE_UE_RAN_QUEUEING")
    upf_strict = False if spec.mm1_upf_strict_priority is None else bool(spec.mm1_upf_strict_priority)
    only_flow = True if spec.mm1_only_traffic_flow_sources_emit is None else bool(spec.mm1_only_traffic_flow_sources_emit)
    return eff_mm1, skip_cp, dis_ue_ran, upf_strict, only_flow


def _mm1_bootstrap_sessions(spec: ModularScenarioSpec, sim: Simulation) -> None:
    """Program active PDU sessions + UPF rules without control-plane signalling."""
    smf_ent = next((e for e in sim.entities.values() if isinstance(e, SMF)), None)
    if smf_ent is None:
        raise RuntimeError("MM1 bootstrap requires an SMF entity")
    for i in range(1, spec.num_ues + 1):
        ue_id = f"UE{i}"
        sid = 1
        ue_ent = sim.entities.get(ue_id)
        inter = float(spec.traffic_period_s)
        if isinstance(ue_ent, UE):
            inter = float(ue_ent.traffic_period_s)
        qos = {"qfi": 1, "priority": 1, "inter_packet_s": inter}
        smf_ent.sessions[(ue_id, sid)] = {"state": "ACTIVE", "qos": dict(qos)}
        smf_ent._push_upf_rules(ue_id, sid, qos, trace_id="")
        if isinstance(ue_ent, UE):
            ue_ent.apply_validation_bootstrap(sid, qos)


def _mm1_log_summary(
    sim: Simulation,
    *,
    eff_mm1: bool,
    skip_cp: bool,
    dis_ue_ran: bool,
    upf_strict: bool,
) -> None:
    upf_ns = 0.0
    for ent in sim.entities.values():
        if isinstance(ent, UPF):
            upf_ns = float(ent.node_service_s)
            break
    mu_str = f"{1.0 / upf_ns:.6g}" if upf_ns > 0.0 else "∞"
    ue1 = sim.entities.get("UE1")
    ue_ns = float(getattr(ue1, "node_service_s", 0.0)) if ue1 is not None else 0.0
    ran_id = getattr(ue1, "ran_id", None) if ue1 is not None else None
    ran_ent = sim.entities.get(ran_id) if isinstance(ran_id, str) else None
    ran_ns = float(getattr(ran_ent, "node_service_s", 0.0)) if ran_ent is not None else 0.0
    print(
        "[sixg_sim] MM1 validation: "
        f"{'ON' if eff_mm1 else 'OFF'}\n"
        f"[sixg_sim] Effective service rate μ ≈ {mu_str} pps "
        f"(UPF mean node_service_s={upf_ns:.6g} s; expovariate(1/mean))\n"
        f"[sixg_sim] Control plane: {'skipped (bootstrap PDU + rules)' if skip_cp and eff_mm1 else 'enabled'}\n"
        f"[sixg_sim] UPF strict priority: {'ON' if upf_strict else 'OFF'} "
        f"({'priority + best-effort queues' if upf_strict else 'single FIFO'})\n"
        f"[sixg_sim] UE/gNB queueing: "
        f"{'OFF' if (ue_ns <= 0.0 and ran_ns <= 0.0) else 'ON'} "
        f"(UE node_service_s={ue_ns:.6g} s, sample gNB node_service_s={ran_ns:.6g} s)"
    )


def build_modular_simulation(spec: ModularScenarioSpec) -> Simulation:
    """Instantiate :class:`~sixg_sim.simulation.Simulation` from ``spec``."""
    spec.validate()
    eff_mm1, skip_cp, dis_ue_ran, upf_strict, only_flow_src = _mm1_resolve_bundle(spec)
    flow_sources = {f.src_ue_index for f in spec.traffic_flows}
    kinds = {n.node_id: n.kind for n in spec.nodes}
    adj = _adjacency(spec.links)
    dn_id = next(n.node_id for n in spec.nodes if n.kind == "dn")

    ptr = spec.packet_tracing if spec.packet_tracing is not None else spec.event_logging
    sim = Simulation(
        event_logging=spec.event_logging,
        event_log_max_entries=spec.event_log_max_entries,
        packet_tracing=ptr,
        packet_lifecycle_max_entries=spec.packet_lifecycle_max_entries,
    )
    sim._upf_overload_queue_threshold = spec.upf_overload_threshold
    sim._upf_overload_cp_min_gap_s = float(spec.upf_overload_cp_min_gap_s)
    sim._track_upf_super_metrics = bool(getattr(spec, "track_s3_super_upf_metrics", False))

    entities: dict[str, object] = {}

    for nspec in spec.nodes:
        nid = nspec.node_id
        k = nspec.kind
        if k == "dn":
            dn_ns = float(nspec.node_service_s) if nspec.node_service_s is not None else 0.0
            entities[nid] = DataNetwork(
                nid,
                hairpin_ue_traffic=spec.dn_hairpin_ue_traffic,
                node_service_s=dn_ns,
                queue_capacity=nspec.queue_capacity,
            )
        elif k == "amf":
            smf_id = _pb(nspec, "smf_id", _infer_peer(adj, kinds, nid, "smf", "AMF→SMF"))
            entities[nid] = AMF(nid, smf_id=smf_id, node_service_s=nspec.node_service_s, queue_capacity=nspec.queue_capacity)
        elif k == "smf":
            if "upf_id" in nspec.peer_bindings:
                upf_id = nspec.peer_bindings["upf_id"]
            else:
                upf_id = _infer_peer(adj, kinds, nid, "upf", "SMF→UPF")
            if "pcf_id" in nspec.peer_bindings:
                pcf_id = nspec.peer_bindings["pcf_id"]
            else:
                pcf_id = _infer_peer(adj, kinds, nid, "pcf", "SMF→PCF")
            if "ran_id" in nspec.peer_bindings:
                ran_id = nspec.peer_bindings["ran_id"]
            else:
                ran_id = _infer_smf_ran_id(nspec, adj, kinds)
            if "dn_id" in nspec.peer_bindings:
                dn_rule_id = nspec.peer_bindings["dn_id"]
            else:
                dn_rule_id = dn_id
            smf_send_rules = spec.smf_send_upf_rule_install_packet
            if eff_mm1 and skip_cp:
                smf_send_rules = False
            entities[nid] = SMF(
                nid,
                upf_id=upf_id,
                pcf_id=pcf_id,
                ran_id=ran_id,
                dn_id=dn_rule_id,
                send_upf_rule_install_packet=smf_send_rules,
                rule_plan_fn=spec.smf_rule_plan_fn,
                node_service_s=nspec.node_service_s,
                queue_capacity=nspec.queue_capacity,
            )
        elif k == "pcf":
            qos_map = {f"UE{k}": int(v) for k, v in spec.ue_upf_qos_class_by_index.items()}
            entities[nid] = PCF(
                nid,
                upf_qos_class_by_ue_id=qos_map or None,
                node_service_s=nspec.node_service_s,
                queue_capacity=nspec.queue_capacity,
            )
        elif k == "ran":
            amf_id = _pb(nspec, "amf_id", _infer_peer(adj, kinds, nid, "amf", "RAN→AMF"))
            upf_id = _pb(nspec, "upf_id", _infer_peer(adj, kinds, nid, "upf", "RAN→UPF"))
            ran_ns = nspec.node_service_s if nspec.node_service_s is not None else spec.ran_node_service_s
            if eff_mm1 and dis_ue_ran:
                ran_ns = 0.0
            entities[nid] = RAN(
                nid,
                amf_id=amf_id,
                upf_id=upf_id,
                node_service_s=ran_ns,
                user_plane_dn_marker=dn_id,
                queue_capacity=nspec.queue_capacity,
            )
        elif k == "upf":
            if "dn_id" in nspec.peer_bindings:
                upf_dn = nspec.peer_bindings["dn_id"]
            else:
                upf_dn = _infer_peer(adj, kinds, nid, "dn", "UPF→DN")
            upf_ns = nspec.node_service_s if nspec.node_service_s is not None else spec.upf_user_plane_service_s
            mixed_geom = spec.upf_mixed_approx_validation and not eff_mm1
            bg_cap = 0.0 if eff_mm1 else spec.upf_background_capacity_pct
            if eff_mm1:
                upf_prio_mode = upf_strict
            else:
                upf_prio_mode = not mixed_geom
                if spec.mm1_upf_strict_priority is not None:
                    upf_prio_mode = bool(spec.mm1_upf_strict_priority)
            entities[nid] = UPF(
                nid,
                dn_id=upf_dn,
                background_mode=spec.upf_background_mode,
                background_capacity_pct=bg_cap,
                background_chunk_service_s=spec.upf_background_chunk_service_s,
                background_arrival_slot_s=spec.upf_background_arrival_slot_s,
                background_arrival_p=spec.upf_background_arrival_p,
                background_service_slot_s=spec.upf_background_service_slot_s,
                background_service_geom_p=spec.upf_background_service_geom_p,
                strict_priority=upf_prio_mode,
                mixed_validation_single_queue=mixed_geom,
                mixed_validation_geometric_for_all=mixed_geom,
                mixed_validation_record_sojourns=mixed_geom,
                user_plane_node_service_s=upf_ns,
                dual_user_qos_queues=spec.upf_dual_user_qos_queues,
                queue_capacity=nspec.queue_capacity,
            )
        else:
            raise AssertionError(k)

    for i in range(1, spec.num_ues + 1):
        bp = spec.ue_bernoulli_p_by_index.get(i, spec.ue_bernoulli_p)
        u_def_qos = (
            UpfQosClass(int(spec.ue_upf_qos_class_by_index[i]))
            if i in spec.ue_upf_qos_class_by_index
            else UpfQosClass.BEST_EFFORT
        )
        ran_id, amf_id = spec.ue_attachment.get(i, (spec.default_ran_id, spec.default_amf_id))
        if ran_id not in entities:
            raise ValueError(f"UE{i} ran_id {ran_id!r} not found")
        if amf_id not in entities:
            raise ValueError(f"UE{i} amf_id {amf_id!r} not found")
        ue_ns_kw: dict[str, float] = {}
        if spec.ue_user_plane_node_service_s is not None:
            ue_ns_kw["node_service_s"] = float(spec.ue_user_plane_node_service_s)
        elif eff_mm1 and dis_ue_ran:
            ue_ns_kw["node_service_s"] = 0.0
        elif eff_mm1 and not dis_ue_ran:
            unif = _infer_uniform_user_plane_service_s(spec)
            if unif is not None:
                ue_ns_kw["node_service_s"] = unif
        ue_q_extra: dict[str, Any] = {}
        if spec.ue_queue_capacity is not None:
            ue_q_extra["queue_capacity"] = spec.ue_queue_capacity
        is_ur = bool(spec.ue_urllc_by_index.get(i, False))
        urate = float(spec.urlcc_rate_pps) if (is_ur and spec.urlcc_rate_pps > 0.0) else None
        ue = UE(
            f"UE{i}",
            amf_id=amf_id,
            ran_id=ran_id,
            traffic_period_s=spec.traffic_period_s,
            registration_jitter_s=spec.ue_registration_jitter_s,
            traffic_period_jitter_relative=spec.ue_traffic_period_jitter_relative,
            bernoulli_interarrival=spec.ue_bernoulli_interarrival,
            bernoulli_p=float(bp),
            poisson_arrival_rate=spec.ue_poisson_arrival_rate,
            default_upf_qos_class=u_def_qos,
            user_traffic_dst_id=dn_id,
            traffic_next_delay_s=spec.traffic_next_delay_s,
            **ue_ns_kw,
            **ue_q_extra,
            urllc=is_ur,
            urllc_rate_pps=urate,
        )
        entities[f"UE{i}"] = ue

    for flow in spec.traffic_flows:
        ue_ent = entities[f"UE{flow.src_ue_index}"]
        if not isinstance(ue_ent, UE):
            raise TypeError(f"traffic_flows: UE{flow.src_ue_index} is not a UE entity")
        rate_eff = traffic_flow_effective_rate_pps(flow)
        ue_ent.user_traffic_dst = flow.dst
        ue_ent.user_traffic_size_bytes = flow.size_bytes
        ue_ent.user_traffic_arrival_process = flow.arrival_process
        if rate_eff is not None:
            if rate_eff <= 0.0:
                raise ValueError(f"traffic_flows: effective rate must be > 0 (got {rate_eff})")
            ue_ent.user_traffic_flow_rate_pps = rate_eff
            ue_ent.traffic_period_s = 1.0 / rate_eff
        else:
            ue_ent.user_traffic_flow_rate_pps = None

    for i in range(1, spec.num_ues + 1):
        ue_ent = entities[f"UE{i}"]
        if isinstance(ue_ent, UE):
            ue_ent.mm1_skip_control_plane = bool(eff_mm1 and skip_cp)
            if eff_mm1 and only_flow_src and i not in flow_sources:
                ue_ent.suppress_user_plane_sources = True

    for ent in entities.values():
        sim.register_entity(ent)  # type: ignore[arg-type]

    for L in spec.links:
        ea = entities[L.endpoint_a]
        eb = entities[L.endpoint_b]
        _connect(ea, eb, float(L.latency_s))  # type: ignore[arg-type]

    ue_lp = LinkProfile(
        ue_ran=spec.ue_ran_base_latency_s,
        ue_ran_latency_jitter_s=spec.ue_ran_latency_jitter_s,
    )
    for i in range(1, spec.num_ues + 1):
        ran_id, _ = spec.ue_attachment.get(i, (spec.default_ran_id, spec.default_amf_id))
        _connect(
            entities[f"UE{i}"],
            entities[ran_id],
            sample_ue_ran_link_latency(ue_lp),
        )  # type: ignore[arg-type]

    if eff_mm1 and skip_cp:
        _mm1_bootstrap_sessions(spec, sim)

    if eff_mm1 and skip_cp:
        for ent in sim.entities.values():
            if isinstance(ent, AMF):
                ent.skip_control_plane_dispatch = True

    ensure_default_control_plane(sim)

    iv = getattr(spec, "cp_strategic_dummy_interval_s", None)
    if iv is not None and float(iv) > 0.0:
        setattr(sim, "_cp_strategic_dummy_interval_s", float(iv))

    _mm1_log_summary(
        sim,
        eff_mm1=eff_mm1,
        skip_cp=skip_cp,
        dis_ue_ran=dis_ue_ran,
        upf_strict=upf_strict,
    )

    for i in range(1, spec.num_ues + 1):
        entities[f"UE{i}"].start()  # type: ignore[union-attr]

    for ns in spec.nodes:
        if ns.kind == "upf":
            entities[ns.node_id].start_background_flow()  # type: ignore[union-attr]

    return sim


def default_scenario_py_path() -> Path:
    """Filesystem path to the built-in default scenario module (``examples/demo_simple.py``)."""
    import sixg_sim.examples as ex

    return Path(next(iter(ex.__path__))) / "demo_simple.py"


def load_modular_scenario_py(path: str | Path) -> Simulation:
    """Load ``path`` (``.py``). Uses ``build_simulation()`` if defined, else ``SCENARIO`` / ``MODULAR_SCENARIO``."""
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    mod_name = "sixg_user_modular_" + path.stem.replace(".", "_")
    loader_spec = importlib.util.spec_from_file_location(mod_name, path)
    if loader_spec is None or loader_spec.loader is None:
        raise ImportError(f"cannot load {path}")
    module = importlib.util.module_from_spec(loader_spec)
    loader_spec.loader.exec_module(module)
    if callable(getattr(module, "build_simulation", None)):
        sim = module.build_simulation()
        if not isinstance(sim, Simulation):
            raise TypeError("build_simulation() must return a Simulation instance")
        ensure_default_control_plane(sim)
        return sim
    scenario = getattr(module, "SCENARIO", None)
    if scenario is None:
        scenario = getattr(module, "MODULAR_SCENARIO", None)
    if isinstance(scenario, ModularScenarioSpec):
        return build_modular_simulation(scenario)
    raise ValueError(
        f"{path}: define build_simulation() -> Simulation or SCENARIO: ModularScenarioSpec "
        "(MODULAR_SCENARIO is accepted as a legacy name)"
    )
