from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Literal

from sixg_sim.control_plane import ensure_default_control_plane
from sixg_sim.core import UpfQosClass
from sixg_sim.entities import AMF, DataNetwork, PCF, RAN, SMF, UE, UPF
from sixg_sim.simulation import Entity, Link, Simulation


@dataclass
class LinkProfile:
    ue_ran: float = 100e-6
    # Per UE–RAN link: latency = ue_ran + U(-jitter, +jitter) sampled once at topology build (each UE differs).
    ue_ran_latency_jitter_s: float = 0.0
    ran_amf: float = 0.002
    amf_smf: float = 0.001
    smf_pcf: float = 0.001
    smf_upf: float = 0.001
    ran_upf: float = 100e-6
    upf_dn: float = 0.005


@dataclass
class ScenarioConfig:
    """High-level parameters for a star RAN topology with one AMF/SMF/PCF/UPF/DN."""

    num_ues: int = 1
    # When False, UEs are not started (no registration / PDU / user traffic); useful for CP-only tests.
    ue_auto_start: bool = True
    traffic_period_s: float = 0.01
    # Per-UE randomness (default 0 = deterministic sync at start).
    ue_registration_jitter_s: float = 0.0
    ue_traffic_period_jitter_relative: float = 0.0
    # Discrete-time Bernoulli arrivals: each slot of length traffic_period_s (or inter_packet_s), send PDU with prob ue_bernoulli_p.
    ue_bernoulli_interarrival: bool = False
    ue_bernoulli_p: float = 0.5
    # Optional per-UE Bernoulli probability (1-based UE index). Missing indices use ``ue_bernoulli_p``.
    ue_bernoulli_p_by_index: dict[int, float] = field(default_factory=dict)
    # Optional UPF user-plane QoS class per UE index: ``0`` = BEST_EFFORT, ``1`` = PRIORITY (strict queue).
    ue_upf_qos_class_by_index: dict[int, int] = field(default_factory=dict)
    # If True, ``UpfQosClass.PRIORITY`` user PDUs use the same strict-priority ingress as ``UPF_BACKGROUND_WORK``.
    upf_dual_user_qos_queues: bool = False
    # Continuous-time Poisson user PDU arrivals at rate λ (1/s); incompatible with ``ue_bernoulli_interarrival``.
    ue_poisson_arrival_rate: float | None = None
    # RAN ingress processing time (s); ``0`` = immediate forward (validation). ``None`` = default (~1e-5).
    ran_node_service_s: float | None = None
    # UPF deterministic service time (s) for user-plane PDUs (and default chunk size baseline). ``None`` = built-in forwarding time.
    upf_user_plane_service_s: float | None = None
    # Finite ingress buffer: max **waiting** packets (not including packet in service), per NF class.
    # ``None`` selects built-in defaults applied in :func:`build_scenario` (UPF=50, RAN=20, UE=10).
    upf_queue_capacity: int | None = None
    ran_queue_capacity: int | None = None
    ue_queue_capacity: int | None = None
    # If False, SMF calls :meth:`UPF.apply_install_rules` directly (no ``UPF_INSTALL_RULES`` packet to the UPF).
    smf_send_upf_rule_install_packet: bool = True
    link: LinkProfile = field(default_factory=LinkProfile)
    # UPF internal background (priority queue): off unless ``upf_background_capacity_pct > 0``.
    # ``poisson``: exponential inter-arrivals at offered load ``capacity_pct``. ``bernoulli_geom``: slot Bernoulli arrivals + geometric service.
    upf_background_mode: Literal["poisson", "bernoulli_geom"] = "poisson"
    upf_background_capacity_pct: float = 0.0
    upf_background_chunk_service_s: float | None = None
    upf_background_arrival_slot_s: float = 100e-6
    upf_background_arrival_p: float = 0.3
    upf_background_service_slot_s: float = 10e-6
    upf_background_service_geom_p: float = 0.5
    # Approximate Geo/Geo/1(total) validation: one UPF FIFO, geometric service for all packets, record sojourns.
    upf_mixed_approx_validation: bool = False
    event_logging: bool = True
    event_log_max_entries: int = 50_000
    packet_tracing: bool | None = None
    # Finite-queue S3 / modular: UPF queue depth overload CP (set threshold on Simulation).
    upf_overload_threshold: int | None = None
    upf_overload_cp_min_gap_s: float = 30.0
    #: Per-UE 1-based index -> True marks DES URLLC traffic at :attr:`urlcc_rate_pps`.
    ue_urllc_by_index: dict[int, bool] = field(default_factory=dict)
    #: Poisson rate (1/s) for each UE with ``ue_urllc_by_index[i]==True`` (DES timers only).
    urlcc_rate_pps: float = 0.0
    packet_lifecycle_max_entries: int = 500_000

    def validate(self) -> None:
        if self.num_ues < 1:
            raise ValueError("num_ues must be >= 1")
        if self.upf_background_mode not in ("poisson", "bernoulli_geom"):
            raise ValueError("upf_background_mode must be 'poisson' or 'bernoulli_geom'")
        if self.upf_background_mode == "poisson":
            if not 0.0 <= self.upf_background_capacity_pct <= 100.0:
                raise ValueError("upf_background_capacity_pct must be in [0, 100]")
            if self.upf_background_chunk_service_s is not None and self.upf_background_chunk_service_s <= 0:
                raise ValueError("upf_background_chunk_service_s must be > 0 when set")
        else:
            if self.upf_background_arrival_slot_s <= 0:
                raise ValueError("upf_background_arrival_slot_s must be > 0 for bernoulli_geom")
            if not 0.0 <= self.upf_background_arrival_p <= 1.0:
                raise ValueError("upf_background_arrival_p must be in [0, 1]")
            if self.upf_background_service_slot_s <= 0:
                raise ValueError("upf_background_service_slot_s must be > 0 for bernoulli_geom")
            if not 0.0 < self.upf_background_service_geom_p <= 1.0:
                raise ValueError("upf_background_service_geom_p must be in (0, 1]")
        if self.ue_bernoulli_interarrival and not 0.0 < self.ue_bernoulli_p <= 1.0:
            raise ValueError("ue_bernoulli_p must be in (0, 1] when ue_bernoulli_interarrival is True")
        if self.ue_bernoulli_p_by_index:
            if not self.ue_bernoulli_interarrival:
                raise ValueError("ue_bernoulli_p_by_index requires ue_bernoulli_interarrival=True")
            for idx, p in self.ue_bernoulli_p_by_index.items():
                if idx < 1 or idx > self.num_ues:
                    raise ValueError(f"ue_bernoulli_p_by_index key {idx} out of range for num_ues={self.num_ues}")
                if not 0.0 < p <= 1.0:
                    raise ValueError(f"ue_bernoulli_p_by_index[{idx}] must be in (0, 1]")
        for idx, qc in self.ue_upf_qos_class_by_index.items():
            if idx < 1 or idx > self.num_ues:
                raise ValueError(f"ue_upf_qos_class_by_index key {idx} out of range for num_ues={self.num_ues}")
            if int(qc) not in (0, 1):
                raise ValueError(f"ue_upf_qos_class_by_index[{idx}] must be 0 (BEST_EFFORT) or 1 (PRIORITY)")
        if self.ue_poisson_arrival_rate is not None:
            if self.ue_poisson_arrival_rate <= 0.0:
                raise ValueError("ue_poisson_arrival_rate must be > 0 when set")
            if self.ue_bernoulli_interarrival:
                raise ValueError("ue_poisson_arrival_rate cannot be combined with ue_bernoulli_interarrival")
        if self.ran_node_service_s is not None and self.ran_node_service_s < 0.0:
            raise ValueError("ran_node_service_s must be >= 0 when set")
        if self.upf_user_plane_service_s is not None and self.upf_user_plane_service_s <= 0.0:
            raise ValueError("upf_user_plane_service_s must be > 0 when set")
        for qc_name, qc_val in (
            ("upf_queue_capacity", self.upf_queue_capacity),
            ("ran_queue_capacity", self.ran_queue_capacity),
            ("ue_queue_capacity", self.ue_queue_capacity),
        ):
            if qc_val is not None and (not isinstance(qc_val, int) or qc_val < 1):
                raise ValueError(f"{qc_name} must be None or an integer >= 1")
        if self.upf_mixed_approx_validation:
            if self.upf_background_mode != "bernoulli_geom":
                raise ValueError("upf_mixed_approx_validation requires upf_background_mode='bernoulli_geom'")
            if not self.ue_bernoulli_interarrival:
                raise ValueError("upf_mixed_approx_validation requires ue_bernoulli_interarrival=True")
            d_a = float(self.upf_background_arrival_slot_s)
            d_s = float(self.upf_background_service_slot_s)
            d_ue = float(self.traffic_period_s)
            scale = max(d_a, d_s, d_ue, 1e-15)
            if abs(d_a - d_s) > 1e-9 * scale or abs(d_a - d_ue) > 1e-9 * scale:
                raise ValueError(
                    "upf_mixed_approx_validation requires traffic_period_s, upf_background_arrival_slot_s, "
                    "and upf_background_service_slot_s to be equal (common slot Δ)"
                )
        if self.urlcc_rate_pps < 0.0:
            raise ValueError("urlcc_rate_pps must be >= 0")
        for idx in self.ue_urllc_by_index:
            if idx < 1 or idx > self.num_ues:
                raise ValueError(f"ue_urllc_by_index key {idx} out of range for num_ues={self.num_ues}")
        if self.upf_overload_threshold is not None and int(self.upf_overload_threshold) < 1:
            raise ValueError("upf_overload_threshold must be None or an integer >= 1")
        if self.upf_overload_cp_min_gap_s < 0.0:
            raise ValueError("upf_overload_cp_min_gap_s must be >= 0")


def _connect(a: Entity, b: Entity, latency: float) -> None:
    link = Link(latency)
    a.connect(b, link)
    b.connect(a, link)


def effective_queue_capacities(config: ScenarioConfig) -> tuple[int, int, int]:
    """Resolve finite-queue sizes: explicit config wins; ``None`` uses UPF=50, RAN=20, UE=10."""
    u = 50 if config.upf_queue_capacity is None else int(config.upf_queue_capacity)
    r = 20 if config.ran_queue_capacity is None else int(config.ran_queue_capacity)
    ue = 10 if config.ue_queue_capacity is None else int(config.ue_queue_capacity)
    return u, r, ue


def sample_ue_ran_link_latency(lp: LinkProfile) -> float:
    """Nominal UE–RAN delay plus uniform radio jitter (one independent draw per UE at build time)."""
    j = float(lp.ue_ran_latency_jitter_s)
    base = float(lp.ue_ran)
    if j <= 0.0:
        return base
    return max(1e-9, base + random.uniform(-j, j))


def build_scenario(config: ScenarioConfig) -> Simulation:
    config.validate()
    upf_q, ran_q, ue_q = effective_queue_capacities(config)
    lp = config.link
    ptr = config.packet_tracing if config.packet_tracing is not None else config.event_logging
    sim = Simulation(
        event_logging=config.event_logging,
        event_log_max_entries=config.event_log_max_entries,
        packet_tracing=ptr,
        packet_lifecycle_max_entries=config.packet_lifecycle_max_entries,
    )

    ues: list[UE] = []
    pcf_qos_map = {f"UE{k}": int(v) for k, v in config.ue_upf_qos_class_by_index.items()}
    for i in range(1, config.num_ues + 1):
        bp = config.ue_bernoulli_p_by_index.get(i, config.ue_bernoulli_p)
        u_def_qos = UpfQosClass(int(config.ue_upf_qos_class_by_index[i])) if i in config.ue_upf_qos_class_by_index else UpfQosClass.BEST_EFFORT
        is_ur = bool(config.ue_urllc_by_index.get(i, False))
        urate = float(config.urlcc_rate_pps) if (is_ur and config.urlcc_rate_pps > 0.0) else None
        ues.append(
            UE(
                f"UE{i}",
                amf_id="AMF1",
                ran_id="RAN1",
                traffic_period_s=config.traffic_period_s,
                registration_jitter_s=config.ue_registration_jitter_s,
                traffic_period_jitter_relative=config.ue_traffic_period_jitter_relative,
                bernoulli_interarrival=config.ue_bernoulli_interarrival,
                bernoulli_p=float(bp),
                poisson_arrival_rate=config.ue_poisson_arrival_rate,
                default_upf_qos_class=u_def_qos,
                queue_capacity=ue_q,
                urllc=is_ur,
                urllc_rate_pps=urate,
            )
        )

    ran = RAN(
        "RAN1",
        amf_id="AMF1",
        upf_id="UPF1",
        node_service_s=config.ran_node_service_s,
        queue_capacity=ran_q,
    )
    amf = AMF("AMF1", smf_id="SMF1")
    smf = SMF(
        "SMF1",
        upf_id="UPF1",
        pcf_id="PCF1",
        ran_id="RAN1",
        send_upf_rule_install_packet=config.smf_send_upf_rule_install_packet,
    )
    pcf = PCF("PCF1", upf_qos_class_by_ue_id=pcf_qos_map or None)
    upf = UPF(
        "UPF1",
        dn_id="DN",
        background_mode=config.upf_background_mode,
        background_capacity_pct=config.upf_background_capacity_pct,
        background_chunk_service_s=config.upf_background_chunk_service_s,
        background_arrival_slot_s=config.upf_background_arrival_slot_s,
        background_arrival_p=config.upf_background_arrival_p,
        background_service_slot_s=config.upf_background_service_slot_s,
        background_service_geom_p=config.upf_background_service_geom_p,
        mixed_validation_single_queue=config.upf_mixed_approx_validation,
        mixed_validation_geometric_for_all=config.upf_mixed_approx_validation,
        mixed_validation_record_sojourns=config.upf_mixed_approx_validation,
        user_plane_node_service_s=config.upf_user_plane_service_s,
        dual_user_qos_queues=config.upf_dual_user_qos_queues,
        queue_capacity=upf_q,
    )
    dn = DataNetwork("DN")

    static_entities: list[Entity] = [ran, amf, smf, pcf, upf, dn]
    for e in (*ues, *static_entities):
        sim.register_entity(e)

    for ue in ues:
        _connect(ue, ran, sample_ue_ran_link_latency(lp))

    _connect(ran, amf, lp.ran_amf)
    _connect(amf, smf, lp.amf_smf)
    _connect(smf, pcf, lp.smf_pcf)
    _connect(smf, upf, lp.smf_upf)
    _connect(ran, upf, lp.ran_upf)
    _connect(upf, dn, lp.upf_dn)

    if config.ue_auto_start:
        for ue in ues:
            ue.start()

    upf.start_background_flow()
    sim._upf_overload_queue_threshold = config.upf_overload_threshold
    sim._upf_overload_cp_min_gap_s = float(config.upf_overload_cp_min_gap_s)
    ensure_default_control_plane(sim)
    return sim
