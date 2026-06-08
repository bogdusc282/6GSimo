from __future__ import annotations

import random
from collections import deque
from collections.abc import Callable
from typing import Literal

from sixg_sim.core import (
    Event,
    Packet,
    PacketPlane,
    TrafficType,
    UpfQosClass,
    coerce_upf_qos_class,
)
from sixg_sim.control_plane import (
    CP_DUMMY_TICK,
    CP_SERVICE_DONE,
    CPDecision,
    CPRequest,
    ControlPlaneBase,
    cp_request_from_packet,
    deliver_cp_completion,
)
from sixg_sim.simulation import NODE_SERVICE_DONE, Entity, Simulation

# Per-NF ingress processing (single-server queue); link latency is separate.
NODE_SERVICE_UPF_FORWARDING_S = 10e-6
NODE_SERVICE_AMF_SMF_S = 3e-3


class DataNetwork(Entity):
    """Data network endpoint: absorbs user-plane packets (optional stats).

    With ``hairpin_ue_traffic=True``, user PDUs whose :attr:`~Packet.dst` names a local
    :class:`UE` are reflected back into the first UPF neighbour as downlink traffic toward
    that UE (toy model for UE--UE reachability via the anchor).
    """

    def __init__(self, eid: str, *, hairpin_ue_traffic: bool = False, node_service_s: float = 0.0, queue_capacity: int | None = None) -> None:
        super().__init__(eid, node_service_s=float(node_service_s), queue_capacity=queue_capacity)
        self.hairpin_ue_traffic = bool(hairpin_ue_traffic)
        self.packets_received: int = 0
        self.bytes_received: int = 0
        self.per_ue_packets: dict[str, int] = {}
        self.per_ue_bytes: dict[str, int] = {}
        self.hairpin_forwards: int = 0

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            self._accept_ingress_packet(event.payload)
            return
        if (
            event.event_type == "TIMER"
            and isinstance(event.payload, dict)
            and event.payload.get("type") == NODE_SERVICE_DONE
        ):
            self._complete_node_service(event.payload["packet"])
            return

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if pkt.plane != PacketPlane.USER:
            return
        dst = pkt.dst
        if (
            self.hairpin_ue_traffic
            and self.sim is not None
            and isinstance(dst, str)
            and dst.startswith("UE")
        ):
            peer = self.sim.entities.get(dst)
            if isinstance(peer, UE):
                if not self.links:
                    return
                nh = next(iter(self.links.keys()))
                self.hairpin_forwards += 1
                reflected = Packet.user_data(
                    ue_id=dst,
                    session_id=1,
                    qos_flow_id=pkt.qos_flow_id or 1,
                    src=self.eid,
                    dst=dst,
                    size_bytes=pkt.size_bytes,
                    traffic_type=pkt.traffic_type,
                    creation_time=self.sim.time,
                    trace_id="",
                    upf_qos_class=pkt.upf_qos_class,
                )
                self.send_packet(nh, reflected)
                return
        self.packets_received += 1
        self.bytes_received += pkt.size_bytes
        self.per_ue_packets[pkt.ue_id] = self.per_ue_packets.get(pkt.ue_id, 0) + 1
        self.per_ue_bytes[pkt.ue_id] = self.per_ue_bytes.get(pkt.ue_id, 0) + pkt.size_bytes


class UE(Entity):
    def __init__(
        self,
        eid: str,
        amf_id: str,
        ran_id: str,
        *,
        traffic_period_s: float = 0.01,
        registration_jitter_s: float = 0.0,
        traffic_period_jitter_relative: float = 0.0,
        bernoulli_interarrival: bool = False,
        bernoulli_p: float = 0.5,
        poisson_arrival_rate: float | None = None,
        default_upf_qos_class: UpfQosClass = UpfQosClass.BEST_EFFORT,
        user_traffic_dst_id: str = "DN",
        user_pdu_size_bytes: int = 1200,
        traffic_next_delay_s: Callable[[Simulation, UE, int], float] | None = None,
        node_service_s: float | None = None,
        queue_capacity: int | None = None,
        urllc: bool = False,
        urllc_rate_pps: float | None = None,
    ) -> None:
        _ns = 1e-5 if node_service_s is None else float(node_service_s)
        super().__init__(eid, node_service_s=_ns, queue_capacity=queue_capacity)
        self.amf_id = amf_id
        self.ran_id = ran_id
        self.user_traffic_dst_id = str(user_traffic_dst_id)
        self.user_pdu_size_bytes = max(1, int(user_pdu_size_bytes))
        self.traffic_next_delay_s = traffic_next_delay_s
        self.user_traffic_flow_rate_pps: float | None = None
        self.user_traffic_arrival_process: Literal["fixed", "poisson"] = "fixed"
        self.default_upf_qos_class = UpfQosClass(int(default_upf_qos_class))
        self.traffic_period_s = traffic_period_s
        self.registration_jitter_s = max(0.0, float(registration_jitter_s))
        self.traffic_period_jitter_relative = max(0.0, float(traffic_period_jitter_relative))
        self.bernoulli_interarrival = bool(bernoulli_interarrival)
        self.bernoulli_p = float(bernoulli_p)
        self.poisson_arrival_rate = float(poisson_arrival_rate) if poisson_arrival_rate is not None else None
        if self.bernoulli_interarrival and not 0.0 < self.bernoulli_p <= 1.0:
            raise ValueError("bernoulli_p must be in (0, 1] when bernoulli_interarrival is True")
        if self.poisson_arrival_rate is not None and self.poisson_arrival_rate <= 0.0:
            raise ValueError("poisson_arrival_rate must be > 0 when set")
        self.registered = False
        self.session_id_counter = 1
        self.sessions: dict[int, dict] = {}
        self._inter_packet_s: dict[int, float] = {}
        self.mm1_skip_control_plane = False
        self.suppress_user_plane_sources = False
        self.urllc = bool(urllc)
        self.urllc_rate_pps = float(urllc_rate_pps) if urllc_rate_pps is not None else None
        if self.urllc and self.urllc_rate_pps is not None and self.urllc_rate_pps <= 0.0:
            raise ValueError("urllc_rate_pps must be > 0 when set for a URLLC UE")

    def apply_validation_bootstrap(self, sid: int, qos: dict) -> None:
        """Pre-load PDU session state when control-plane signalling is skipped (MM1 validation)."""
        self.registered = True
        q = dict(qos)
        self.sessions[int(sid)] = {"session_id": int(sid), "qos": q}
        self._inter_packet_s[int(sid)] = float(q.get("inter_packet_s", self.traffic_period_s))
        self.session_id_counter = max(self.session_id_counter, int(sid) + 1)

    @property
    def user_traffic_dst(self) -> str:
        return self.user_traffic_dst_id

    @user_traffic_dst.setter
    def user_traffic_dst(self, v: str) -> None:
        self.user_traffic_dst_id = str(v)

    @property
    def user_traffic_size_bytes(self) -> int:
        return self.user_pdu_size_bytes

    @user_traffic_size_bytes.setter
    def user_traffic_size_bytes(self, v: int) -> None:
        self.user_pdu_size_bytes = max(1, int(v))

    def start(self) -> None:
        if self.sim is None:
            raise RuntimeError("UE not registered")
        if self.suppress_user_plane_sources:
            return
        if self.mm1_skip_control_plane:
            for sid in list(self.sessions.keys()):
                self._schedule_next_packet(int(sid))
            return
        if self.registration_jitter_s > 0.0:
            delay = random.random() * self.registration_jitter_s
            self.sim.schedule(
                time=self.sim.time + delay,
                priority=2,
                target=self,
                event_type="TIMER",
                payload={"type": "UE_START_REGISTRATION"},
            )
        else:
            self._send_registration_request()

    def _send_registration_request(self) -> None:
        if self.sim is None:
            return
        pkt = Packet.control_signal(
            "NAS_REGISTRATION_REQUEST",
            src=self.eid,
            dst=self.amf_id,
            payload={"ue_id": self.eid},
            creation_time=self.sim.time,
        )
        self.send_packet(self.ran_id, pkt)

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            pkt = event.payload
            if (
                isinstance(pkt, Packet)
                and pkt.plane == PacketPlane.USER
                and self.sim is not None
            ):
                self.sim.trace_packet(
                    "PACKET_ARRIVAL",
                    pkt,
                    at_entity=self.eid,
                    detail="user-plane pdu arrival",
                )
            self._accept_ingress_packet(pkt)
            return
        if (
            event.event_type == "TIMER"
            and isinstance(event.payload, dict)
            and event.payload.get("type") == NODE_SERVICE_DONE
        ):
            self._complete_node_service(event.payload["packet"])
            return
        if event.event_type == "TIMER":
            self._handle_timer(event.payload)
            return

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if pkt.plane == PacketPlane.CONTROL:
            self._handle_control_packet(pkt)
        else:
            self._handle_user_packet(pkt)

    def _handle_control_packet(self, pkt: Packet) -> None:
        if pkt.msg_type == "NAS_REGISTRATION_ACCEPT":
            self.registered = True
            self._request_pdu_session()
        elif pkt.msg_type == "PDU_SESSION_ESTABLISHMENT_ACCEPT":
            sid = int(pkt.control_payload["session_id"])
            self.sessions[sid] = dict(pkt.control_payload)
            qos = dict(pkt.control_payload.get("qos", {}))
            self._inter_packet_s[sid] = float(qos.get("inter_packet_s", self.traffic_period_s))
            self._schedule_next_packet(sid)
        elif pkt.msg_type == "PDU_SESSION_QOS_NOTIFY":
            sid = int(pkt.control_payload["session_id"])
            if sid not in self.sessions:
                return
            qos = dict(pkt.control_payload.get("qos", {}))
            if "inter_packet_s" in qos:
                self._inter_packet_s[sid] = float(qos["inter_packet_s"])
            q = self.sessions[sid].setdefault("qos", {})
            if isinstance(q, dict):
                q.update(qos)

    def _request_pdu_session(self) -> None:
        if self.sim is None:
            return
        sid = self.session_id_counter
        self.session_id_counter += 1
        pkt = Packet.control_signal(
            "PDU_SESSION_ESTABLISHMENT_REQUEST",
            src=self.eid,
            dst=self.amf_id,
            payload={"ue_id": self.eid, "session_id": sid},
            creation_time=self.sim.time,
        )
        self.send_packet(self.ran_id, pkt)

    def _slot_duration_s(self, session_id: int) -> float:
        return float(self._inter_packet_s.get(session_id, self.traffic_period_s))

    def _emit_user_pdu(self, session_id: int) -> None:
        if self.sim is None:
            return
        sess = self.sessions.get(session_id, {})
        qos = dict(sess["qos"]) if isinstance(sess.get("qos"), dict) else {}
        u_cls = coerce_upf_qos_class(qos.get("upf_qos_class"), self.default_upf_qos_class)
        pkt = Packet.user_data(
            ue_id=self.eid,
            session_id=session_id,
            qos_flow_id=1,
            src=self.eid,
            dst=self.user_traffic_dst_id,
            size_bytes=self.user_pdu_size_bytes,
            traffic_type=TrafficType.NORMAL,
            creation_time=self.sim.time,
            upf_qos_class=u_cls,
            qos="URLLC" if self.urllc else "",
        )
        self.sim.trace_packet(
            "PACKET_CREATION",
            pkt,
            at_entity=self.eid,
            peer=self.ran_id,
            detail="user-plane pdu emit",
        )
        self._accept_ingress_packet(pkt)

    def _schedule_next_packet(self, session_id: int) -> None:
        if self.sim is None:
            return
        if self.urllc and self.urllc_rate_pps is not None and self.urllc_rate_pps > 0.0:
            lam = float(self.urllc_rate_pps)
            dt = max(1e-12, random.expovariate(lam))
            self.sim.schedule(
                time=self.sim.time + dt,
                priority=2,
                target=self,
                event_type="TIMER",
                payload={"type": "GENERATE_PACKET", "session_id": session_id},
            )
            return
        if self.traffic_next_delay_s is not None:
            dt = max(1e-12, float(self.traffic_next_delay_s(self.sim, self, session_id)))
            self.sim.schedule(
                time=self.sim.time + dt,
                priority=2,
                target=self,
                event_type="TIMER",
                payload={"type": "GENERATE_PACKET", "session_id": session_id},
            )
            return
        if self.user_traffic_flow_rate_pps is not None and self.user_traffic_flow_rate_pps > 0:
            lam = float(self.user_traffic_flow_rate_pps)
            if self.user_traffic_arrival_process == "poisson":
                dt = max(1e-12, random.expovariate(lam))
            else:
                dt = max(1e-12, 1.0 / lam)
            self.sim.schedule(
                time=self.sim.time + dt,
                priority=2,
                target=self,
                event_type="TIMER",
                payload={"type": "GENERATE_PACKET", "session_id": session_id},
            )
            return
        if self.poisson_arrival_rate is not None:
            dt = random.expovariate(self.poisson_arrival_rate)
            self.sim.schedule(
                time=self.sim.time + dt,
                priority=2,
                target=self,
                event_type="TIMER",
                payload={"type": "GENERATE_PACKET", "session_id": session_id},
            )
            return
        if self.bernoulli_interarrival:
            slot = self._slot_duration_s(session_id)
            if slot <= 0.0:
                return
            self.sim.schedule(
                time=self.sim.time + slot,
                priority=2,
                target=self,
                event_type="TIMER",
                payload={"type": "BERNOULLI_SLOT", "session_id": session_id},
            )
            return
        period = self._slot_duration_s(session_id)
        if self.traffic_period_jitter_relative > 0.0:
            r = self.traffic_period_jitter_relative
            period *= random.uniform(max(0.05, 1.0 - r), 1.0 + r)
        self.sim.schedule(
            time=self.sim.time + period,
            priority=2,
            target=self,
            event_type="TIMER",
            payload={"type": "GENERATE_PACKET", "session_id": session_id},
        )

    def _handle_timer(self, payload: dict) -> None:
        if payload["type"] == "UE_START_REGISTRATION" and self.sim is not None:
            self._send_registration_request()
            return
        if payload["type"] == "BERNOULLI_SLOT" and self.sim is not None:
            sid = int(payload["session_id"])
            if random.random() < self.bernoulli_p:
                self._emit_user_pdu(sid)
            self._schedule_next_packet(sid)
            return
        if payload["type"] == "GENERATE_PACKET" and self.sim is not None:
            sid = payload["session_id"]
            self._emit_user_pdu(sid)
            self._schedule_next_packet(sid)

    def _handle_user_packet(self, packet: Packet) -> None:
        """After UE node processing: absorb DL sink PDUs; forward locally originated UL PDUs to RAN."""
        if packet.dst == self.eid:
            return
        if packet.ue_id == self.eid and packet.src == self.eid:
            self.send_packet(self.ran_id, packet)


class RAN(Entity):
    def __init__(
        self,
        eid: str,
        amf_id: str,
        upf_id: str,
        *,
        node_service_s: float | None = None,
        user_plane_dn_marker: str = "DN",
        queue_capacity: int | None = None,
    ) -> None:
        ns = float(node_service_s) if node_service_s is not None else 1e-5
        super().__init__(eid, node_service_s=ns, queue_capacity=queue_capacity)
        self.amf_id = amf_id
        self.upf_id = upf_id
        self.user_plane_dn_marker = str(user_plane_dn_marker)

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            self._accept_ingress_packet(event.payload)
            return
        if (
            event.event_type == "TIMER"
            and isinstance(event.payload, dict)
            and event.payload.get("type") == NODE_SERVICE_DONE
        ):
            self._complete_node_service(event.payload["packet"])
            return

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if pkt.plane == PacketPlane.CONTROL:
            if pkt.dst == self.amf_id:
                pkt.src = self.eid
                self.send_packet(self.amf_id, pkt)
            else:
                self.send_packet(pkt.dst, pkt)
            return
        if pkt.dst == self.user_plane_dn_marker:
            self.send_packet(self.upf_id, pkt)
            return
        if pkt.dst in self.links:
            self.send_packet(pkt.dst, pkt)
            return
        if pkt.src in self.links:
            self.send_packet(self.upf_id, pkt)
            return
        self.send_packet(pkt.dst, pkt)


class UPF(Entity):
    """User-plane forwarding + optional internal background (priority queue).

    Two ingress FIFOs share one server: **priority** (synthetic ``UPF_BACKGROUND_WORK`` and, when
    ``dual_user_qos_queues`` is True, user PDUs with :class:`~sixg_sim.core.UpfQosClass.PRIORITY`) is always
    served before **best-effort** (remaining UE user-plane and SMF control such as rule install).
    Non-preemptive: the packet in service always completes.

    Background modes (both require ``background_capacity_pct > 0`` to run):
    - **poisson**: exponential inter-arrivals; fixed (or configured) chunk service time; ``capacity_pct`` sets offered load.
    - **bernoulli_geom**: discrete slots of ``background_arrival_slot_s``, Bernoulli arrival per slot; service time is
      ``K * background_service_slot_s`` with ``K`` geometric on ``{1,2,...}`` (success prob
      ``background_service_geom_p`` per quantum, mean ``1/background_service_geom_p``). This **K** is the **chunk’s**
      own random service length in quanta. **Other** packets (more background chunks, user plane, control) can **also**
      queue at the UPF and add delay on top—whether that is simulated depends on the scenario (e.g. mixed-validation
      may draw **K·Δ** for a PDU in one timer rather than interleaving per-quantum competitors).
    """

    def __init__(
        self,
        eid: str,
        dn_id: str,
        *,
        background_mode: Literal["poisson", "bernoulli_geom"] = "poisson",
        background_capacity_pct: float = 0.0,
        background_chunk_service_s: float | None = None,
        background_arrival_slot_s: float = 100e-6,
        background_arrival_p: float = 0.3,
        background_service_slot_s: float = NODE_SERVICE_UPF_FORWARDING_S,
        background_service_geom_p: float = 0.5,
        strict_priority: bool = True,
        mixed_validation_single_queue: bool = False,
        mixed_validation_geometric_for_all: bool = False,
        mixed_validation_record_sojourns: bool = False,
        user_plane_node_service_s: float | None = None,
        dual_user_qos_queues: bool = False,
        queue_capacity: int | None = None,
    ) -> None:
        _ns = (
            float(user_plane_node_service_s)
            if user_plane_node_service_s is not None and float(user_plane_node_service_s) > 0.0
            else NODE_SERVICE_UPF_FORWARDING_S
        )
        super().__init__(eid, node_service_s=_ns, queue_capacity=queue_capacity)
        #: Nominal (healthy) user-plane mean service time; restored after ``RECONFIGURE_UPF``.
        self._nominal_node_service_s = float(self.node_service_s)
        #: True while overload-CP has put this UPF in degraded (slow) service mode.
        self._overload_degraded_active = False
        #: Mirrors whether the UPF is currently in overload-induced degraded mode (``True`` = bad mode).
        self.is_overloaded = False
        self.dn_id = dn_id
        self.fwd_table: dict[tuple[str, int, str], str] = {}
        self.session_qos: dict[tuple[str, int, str], dict] = {}
        self.background_mode: Literal["poisson", "bernoulli_geom"] = background_mode
        self.background_capacity_pct = float(max(0.0, min(100.0, background_capacity_pct)))
        self._background_chunk_service_s = background_chunk_service_s
        self.background_arrival_slot_s = float(background_arrival_slot_s)
        self.background_arrival_p = float(background_arrival_p)
        self.background_service_slot_s = float(background_service_slot_s)
        self.background_service_geom_p = float(background_service_geom_p)
        if mixed_validation_single_queue:
            strict_priority = False
        self.strict_priority = bool(strict_priority)
        self.mixed_validation_single_queue = bool(mixed_validation_single_queue)
        self.mixed_validation_geometric_for_all = bool(mixed_validation_geometric_for_all)
        self.mixed_validation_record_sojourns = bool(mixed_validation_record_sojourns)
        self.dual_user_qos_queues = bool(dual_user_qos_queues)
        self.background_chunks_completed = 0
        # (chunk_creation_time_s, sojourn_s) for Poisson/geom background work units (validation / diagnostics).
        self.background_chunk_sojourn_samples: list[tuple[float, float]] = []
        # (upf_ingress_time_s, sojourn_s, kind) kind in {"bg","user","ctrl"} — mixed approximate validation.
        self.upf_all_sojourn_samples: list[tuple[float, float, str]] = []
        # User-plane PDU: UPF ingress → end of UPF service (UE M/D/1 validation).
        # (upf_ingress_time_s, sojourn_s, ue_id) for user-plane PDU completions.
        self.ue_data_sojourn_samples: list[tuple[float, float, str]] = []
        self._ingress_priority: deque[Packet] = deque()
        self._ingress_best_effort: deque[Packet] = deque()

    def _waiting_depth(self) -> int:
        return len(self._ingress_priority) + len(self._ingress_best_effort)

    def enter_overload_degraded(self) -> None:
        """CP overload path: slow the UPF (``node_service_s`` × 2) until reconfiguration completes."""
        if self._overload_degraded_active:
            return
        self.node_service_s = float(self._nominal_node_service_s) * 2.0
        self.is_overloaded = True
        self._overload_degraded_active = True

    def apply_reconfigure_upf(self) -> None:
        """Applied when CP decides ``RECONFIGURE_UPF``: restore nominal service; clear overload flag."""
        self.node_service_s = float(self._nominal_node_service_s)
        self.is_overloaded = False
        self._overload_degraded_active = False

    def _complete_node_service(self, pkt: Packet) -> None:
        if self.sim is not None:
            self.sim.trace_packet("NODE_END", pkt, at_entity=self.eid)
        self._node_busy = False
        if pkt.plane == PacketPlane.USER and self.sim is not None:
            pkt.upf_egress_time = float(self.sim.time)
            t0 = float(pkt.upf_ingress_time) if pkt.upf_ingress_time is not None else float(pkt.creation_time)
            lat = float(self.sim.time - t0)
            self.sim.trace_upf_latency(pkt, latency_upf_s=lat, at_entity=self.eid)
        self._route_packet_after_node(pkt)
        self._drain_node_server()

    def _is_priority_packet(self, pkt: Packet) -> bool:
        if not self.strict_priority:
            return False
        if pkt.plane == PacketPlane.CONTROL and pkt.msg_type == "UPF_BACKGROUND_WORK":
            return True
        if (
            self.dual_user_qos_queues
            and pkt.plane == PacketPlane.USER
            and pkt.upf_qos_class == UpfQosClass.PRIORITY
        ):
            return True
        return False

    def _pop_next_for_service(self) -> Packet:
        if self._ingress_priority:
            return self._ingress_priority.popleft()
        return self._ingress_best_effort.popleft()

    def _accept_ingress_packet(self, pkt: Packet) -> None:
        qc = self.queue_capacity
        if qc is not None and self._waiting_depth() >= qc:
            self.drop_count += 1
            if self.sim is not None:
                target_q = "priority" if self._is_priority_packet(pkt) else "best_effort"
                self.sim.trace_packet(
                    "DROP",
                    pkt,
                    at_entity=self.eid,
                    detail=(
                        f"reason=queue_full queue_capacity={qc} "
                        f"waiting_total={self._waiting_depth()} target_queue={target_q}"
                    ),
                )
                self._queue_stats_on_drop()
            return
        if self.sim is not None:
            pkt.upf_ingress_time = self.sim.time
        if self._is_priority_packet(pkt):
            self._ingress_priority.append(pkt)
            qname, depth = "priority", len(self._ingress_priority)
        else:
            self._ingress_best_effort.append(pkt)
            qname, depth = "best_effort", len(self._ingress_best_effort)
        self._queue_stats_after_mutation("enqueue")
        if self.sim is not None:
            self.sim.trace_packet(
                "INGRESS",
                pkt,
                at_entity=self.eid,
                detail=f"queue={qname} depth_after={depth}",
            )
        self._drain_node_server()

    def _drain_node_server(self) -> None:
        if self._node_busy or self.sim is None:
            return
        if not self._ingress_priority and not self._ingress_best_effort:
            return
        if self.node_service_s <= 0.0:
            pkt = self._pop_next_for_service()
            self._queue_stats_after_mutation("dequeue")
            self.sim.trace_packet(
                "NODE_PASS",
                pkt,
                at_entity=self.eid,
                detail="node_service_s<=0 immediate route",
            )
            self._route_packet_after_node(pkt)
            self._drain_node_server()
            return
        self._node_busy = True
        pkt = self._pop_next_for_service()
        self._queue_stats_after_mutation("dequeue")
        st = self._node_service_time_for(pkt)
        self.sim.trace_packet(
            "NODE_START",
            pkt,
            at_entity=self.eid,
            detail=f"service_s={st:.6g}",
        )
        self.sim.schedule(
            time=self.sim.time + st,
            priority=2,
            target=self,
            event_type="TIMER",
            payload={"type": NODE_SERVICE_DONE, "packet": pkt},
        )

    def _background_chunk_service(self) -> float:
        if self._background_chunk_service_s is not None and float(self._background_chunk_service_s) > 0:
            return float(self._background_chunk_service_s)
        return self.node_service_s

    @staticmethod
    def _sample_geometric_trials_until_success(p: float) -> int:
        """Trials until first success, support {1,2,...}, P(K=k)=(1-p)^(k-1)*p, E[K]=1/p."""
        k = 0
        while True:
            k += 1
            if random.random() < p:
                return k

    def _background_geometric_service_s(self) -> float:
        k = self._sample_geometric_trials_until_success(self.background_service_geom_p)
        return max(1e-12, float(k) * self.background_service_slot_s)

    def _node_service_time_for(self, pkt: Packet) -> float:
        if self.background_mode == "bernoulli_geom" and self.mixed_validation_geometric_for_all:
            return self._background_geometric_service_s()
        if pkt.plane == PacketPlane.CONTROL and pkt.msg_type == "UPF_BACKGROUND_WORK":
            if self.background_mode == "bernoulli_geom":
                return self._background_geometric_service_s()
            mean_s = float(self._background_chunk_service())
            if mean_s <= 0.0:
                return 0.0
            return random.expovariate(1.0 / mean_s)
        mean_s = float(self.node_service_s)
        if mean_s <= 0.0:
            return 0.0
        return random.expovariate(1.0 / mean_s)

    def _background_flow_enabled(self) -> bool:
        """Synthetic UPF background is off unless ``background_capacity_pct > 0`` (both modes)."""
        if self.background_capacity_pct <= 0.0:
            return False
        if self.background_mode == "poisson":
            return True
        return (
            self.background_arrival_slot_s > 0.0
            and 0.0 < self.background_arrival_p <= 1.0
            and self.background_service_slot_s > 0.0
            and 0.0 < self.background_service_geom_p <= 1.0
        )

    def start_background_flow(self) -> None:
        if self.sim is None or not self._background_flow_enabled():
            return
        self._schedule_next_background_tick()

    def _schedule_next_background_tick(self) -> None:
        if self.sim is None or not self._background_flow_enabled():
            return
        if self.background_mode == "poisson":
            s = self._background_chunk_service()
            if s <= 0.0:
                return
            rate = (self.background_capacity_pct / 100.0) / s
            dt = random.expovariate(rate)
            self.sim.schedule(
                time=self.sim.time + dt,
                priority=4,
                target=self,
                event_type="TIMER",
                payload={"type": "UPF_BACKGROUND_TICK"},
            )
            return
        slot = self.background_arrival_slot_s
        self.sim.schedule(
            time=self.sim.time + slot,
            priority=4,
            target=self,
            event_type="TIMER",
            payload={"type": "UPF_BACKGROUND_TICK"},
        )

    def _enqueue_background_chunk(self) -> None:
        if self.sim is None:
            return
        pkt = Packet.control_signal(
            "UPF_BACKGROUND_WORK",
            src=self.eid,
            dst=self.eid,
            payload={},
            creation_time=self.sim.time,
        )
        self._accept_ingress_packet(pkt)

    def _on_background_tick(self) -> None:
        if self.sim is None:
            return
        if self.background_mode == "bernoulli_geom":
            if random.random() < self.background_arrival_p:
                self._enqueue_background_chunk()
            self._schedule_next_background_tick()
            return
        self._enqueue_background_chunk()
        self._schedule_next_background_tick()

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            self._accept_ingress_packet(event.payload)
            return
        if event.event_type == "TIMER" and isinstance(event.payload, dict):
            pl = event.payload
            if pl.get("type") == NODE_SERVICE_DONE:
                self._complete_node_service(pl["packet"])
                return
            if pl.get("type") == "UPF_BACKGROUND_TICK":
                self._on_background_tick()
                return

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if self.mixed_validation_record_sojourns and self.sim is not None:
            t0 = float(pkt.upf_ingress_time) if pkt.upf_ingress_time is not None else float(pkt.creation_time)
            sj_all = self.sim.time - t0
            if pkt.plane == PacketPlane.CONTROL and pkt.msg_type == "UPF_BACKGROUND_WORK":
                kind = "bg"
            elif pkt.plane == PacketPlane.USER:
                kind = "user"
            else:
                kind = "ctrl"
            self.upf_all_sojourn_samples.append((t0, float(sj_all), kind))
        if pkt.plane == PacketPlane.CONTROL and pkt.msg_type == "UPF_BACKGROUND_WORK":
            self.background_chunks_completed += 1
            if self.sim is not None:
                t_ing = float(pkt.upf_ingress_time) if pkt.upf_ingress_time is not None else float(pkt.creation_time)
                sj = self.sim.time - t_ing
                self.background_chunk_sojourn_samples.append((t_ing, float(sj)))
            return
        if pkt.plane == PacketPlane.CONTROL:
            self._handle_control_packet(pkt)
            return
        direction = "UL" if pkt.src == pkt.ue_id else "DL"
        key = (pkt.ue_id, pkt.session_id, direction)
        next_hop = self.fwd_table.get(key)
        if not next_hop:
            return
        if self.sim is not None:
            t0 = float(pkt.upf_ingress_time) if pkt.upf_ingress_time is not None else float(pkt.creation_time)
            uid = str(pkt.ue_id) if getattr(pkt, "ue_id", None) is not None else ""
            self.ue_data_sojourn_samples.append((t0, float(self.sim.time - t0), uid))
        self.send_packet(next_hop, pkt)

    def apply_install_rules(self, rules: list[dict]) -> None:
        """Program forwarding from SMF without sending a control Packet to the UPF (out-of-band rule install)."""
        for r in rules:
            ue_id = r["ue_id"]
            key = (ue_id, r["session_id"], r["direction"])
            self.fwd_table[key] = r["next_hop"]
            if "qos" in r:
                self.session_qos[key] = dict(r["qos"])

    def _handle_control_packet(self, pkt: Packet) -> None:
        if pkt.msg_type == "UPF_INSTALL_RULES":
            self.apply_install_rules(pkt.control_payload["rules"])


class AMF(Entity):
    def __init__(
        self,
        eid: str,
        smf_id: str,
        *,
        node_service_s: float | None = None,
        control_plane: ControlPlaneBase | None = None,
        queue_capacity: int | None = None,
    ) -> None:
        ns = NODE_SERVICE_AMF_SMF_S if node_service_s is None else float(node_service_s)
        super().__init__(eid, node_service_s=ns, queue_capacity=queue_capacity)
        self.smf_id = smf_id
        self.ue_contexts: dict[str, dict] = {}
        self.ue_servicing_ran: dict[str, str] = {}
        self.control_plane: ControlPlaneBase | None = control_plane
        # Set True in MM1 validation+skip_cp so bootstrapped runs avoid CP delay.
        self.skip_control_plane_dispatch: bool = False

    def attach_control_plane(self, cp: ControlPlaneBase) -> None:
        self.control_plane = cp

    def schedule_cp_dummy_triggers(self, interval_s: float, until_s: float) -> None:
        if self.sim is None or interval_s <= 0.0:
            return
        t_next = float(self.sim.time) + float(interval_s)
        if t_next > float(until_s) + 1e-12:
            return
        self.sim.schedule(
            time=t_next,
            priority=3,
            target=self,
            event_type="TIMER",
            payload={
                "type": CP_DUMMY_TICK,
                "interval_s": float(interval_s),
                "until_s": float(until_s),
            },
        )

    @staticmethod
    def _is_access_node(neighbor_id: str) -> bool:
        return neighbor_id.startswith("RAN") or neighbor_id.startswith("gNB")

    def _first_access_neighbor(self) -> str | None:
        for neighbor_id in self.links:
            if self._is_access_node(neighbor_id):
                return neighbor_id
        return None

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            self._accept_ingress_packet(event.payload)
            return
        if event.event_type == "TIMER" and isinstance(event.payload, dict):
            pl = event.payload
            if pl.get("type") == CP_DUMMY_TICK:
                self._on_cp_dummy_tick(pl)
                return
            if pl.get("type") == CP_SERVICE_DONE:
                self._on_cp_service_done(pl)
                return
            if pl.get("type") == NODE_SERVICE_DONE:
                self._complete_node_service(pl["packet"])
                return

    def _control_plane_for_packet(self, pkt: Packet) -> bool:
        if self.control_plane is None or self.skip_control_plane_dispatch:
            return False
        if pkt.plane != PacketPlane.CONTROL:
            return False
        return pkt.msg_type in (
            "NAS_REGISTRATION_REQUEST",
            "PDU_SESSION_ESTABLISHMENT_REQUEST",
            "PDU_SESSION_ESTABLISHMENT_ACCEPT",
            "PDU_SESSION_QOS_NOTIFY",
        )

    def _on_cp_dummy_tick(self, pl: dict) -> None:
        if self.sim is None:
            return
        interval = float(pl["interval_s"])
        until = float(pl["until_s"])
        next_t = self.sim.time + interval
        if next_t <= until + 1e-12:
            self.sim.schedule(
                time=next_t,
                priority=3,
                target=self,
                event_type="TIMER",
                payload={"type": CP_DUMMY_TICK, "interval_s": interval, "until_s": until},
            )
        cp = self.control_plane
        if cp is None:
            return
        cp.process_request(
            CPRequest(ue_id="*", slice_id="*", event_type="CP_DUMMY_HEARTBEAT", kpis={}),
            self,
            original_packet=None,
        )

    def _on_cp_service_done(self, pl: dict) -> None:
        deliver_cp_completion(self, pl)

    def _apply_control_plane_decision(
        self,
        original_packet: Packet | None,
        request: CPRequest,
        decision: CPDecision,
    ) -> None:
        if original_packet is None:
            return
        pkt = original_packet
        if not decision.admit:
            return
        if pkt.plane != PacketPlane.CONTROL:
            return
        if pkt.msg_type == "NAS_REGISTRATION_REQUEST":
            ue_id = pkt.control_payload["ue_id"]
            self.ue_contexts[ue_id] = {"registered": True}
            self.ue_servicing_ran[ue_id] = pkt.src
            resp = Packet.control_signal(
                "NAS_REGISTRATION_ACCEPT",
                src=self.eid,
                dst=ue_id,
                payload={"ue_id": ue_id},
                creation_time=self.sim.time if self.sim else 0.0,
                trace_id=pkt.trace_id,
            )
            self.send_packet(pkt.src, resp)
            return
        if pkt.msg_type == "PDU_SESSION_ESTABLISHMENT_REQUEST":
            if not decision.forward_to_smf:
                return
            ue_id = pkt.control_payload.get("ue_id")
            if isinstance(ue_id, str):
                self.ue_servicing_ran[ue_id] = pkt.src
            fwd = Packet.control_signal(
                "SMF_CREATE_SESSION",
                src=self.eid,
                dst=self.smf_id,
                payload=dict(pkt.control_payload),
                creation_time=self.sim.time if self.sim else 0.0,
                trace_id=pkt.trace_id,
            )
            self.send_packet(self.smf_id, fwd)
            return
        if pkt.msg_type == "PDU_SESSION_ESTABLISHMENT_ACCEPT":
            ue_id = pkt.dst
            ran_id = self.ue_servicing_ran.get(ue_id) or self._first_access_neighbor()
            if ran_id:
                self.send_packet(ran_id, pkt)
            return
        if pkt.msg_type == "PDU_SESSION_QOS_NOTIFY":
            ue_id = pkt.dst
            ran_id = self.ue_servicing_ran.get(ue_id) or self._first_access_neighbor()
            if ran_id:
                self.send_packet(ran_id, pkt)
            return

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if pkt.plane != PacketPlane.CONTROL:
            return
        if self._control_plane_for_packet(pkt):
            assert self.control_plane is not None
            self.control_plane.process_request(cp_request_from_packet(pkt), self, original_packet=pkt)
            return
        if pkt.msg_type == "NAS_REGISTRATION_REQUEST":
            ue_id = pkt.control_payload["ue_id"]
            self.ue_contexts[ue_id] = {"registered": True}
            self.ue_servicing_ran[ue_id] = pkt.src
            resp = Packet.control_signal(
                "NAS_REGISTRATION_ACCEPT",
                src=self.eid,
                dst=ue_id,
                payload={"ue_id": ue_id},
                creation_time=self.sim.time if self.sim else 0.0,
                trace_id=pkt.trace_id,
            )
            self.send_packet(pkt.src, resp)
        elif pkt.msg_type == "PDU_SESSION_ESTABLISHMENT_REQUEST":
            ue_id = pkt.control_payload.get("ue_id")
            if isinstance(ue_id, str):
                self.ue_servicing_ran[ue_id] = pkt.src
            fwd = Packet.control_signal(
                "SMF_CREATE_SESSION",
                src=self.eid,
                dst=self.smf_id,
                payload=dict(pkt.control_payload),
                creation_time=self.sim.time if self.sim else 0.0,
                trace_id=pkt.trace_id,
            )
            self.send_packet(self.smf_id, fwd)
        elif pkt.msg_type == "PDU_SESSION_ESTABLISHMENT_ACCEPT":
            ue_id = pkt.dst
            ran_id = self.ue_servicing_ran.get(ue_id) or self._first_access_neighbor()
            if ran_id:
                self.send_packet(ran_id, pkt)
        elif pkt.msg_type == "PDU_SESSION_QOS_NOTIFY":
            ue_id = pkt.dst
            ran_id = self.ue_servicing_ran.get(ue_id) or self._first_access_neighbor()
            if ran_id:
                self.send_packet(ran_id, pkt)


class SMF(Entity):
    def __init__(
        self,
        eid: str,
        upf_id: str,
        pcf_id: str,
        ran_id: str,
        *,
        dn_id: str = "DN",
        reopt_dst: str | None = None,
        send_upf_rule_install_packet: bool = True,
        rule_plan_fn: Callable[[SMF, str, int, dict], list[tuple[str, list[dict]]]] | None = None,
        node_service_s: float | None = None,
        queue_capacity: int | None = None,
    ) -> None:
        ns = NODE_SERVICE_AMF_SMF_S if node_service_s is None else float(node_service_s)
        super().__init__(eid, node_service_s=ns, queue_capacity=queue_capacity)
        self.upf_id = upf_id
        self.pcf_id = pcf_id
        self.ran_id = ran_id
        self.dn_id = str(dn_id)
        self.reopt_dst = reopt_dst
        self.send_upf_rule_install_packet = bool(send_upf_rule_install_packet)
        self.rule_plan_fn = rule_plan_fn
        self.sessions: dict[tuple[str, int], dict] = {}

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            self._accept_ingress_packet(event.payload)
            return
        if (
            event.event_type == "TIMER"
            and isinstance(event.payload, dict)
            and event.payload.get("type") == NODE_SERVICE_DONE
        ):
            self._complete_node_service(event.payload["packet"])
            return

    def _amf_neighbor(self) -> str | None:
        for neighbor_id in self.links:
            if neighbor_id.startswith("AMF"):
                return neighbor_id
        return None

    def _build_upf_rules(self, ue_id: str, sid: int, qos: dict) -> list[dict]:
        return [
            {
                "ue_id": ue_id,
                "session_id": sid,
                "direction": "UL",
                "next_hop": self.dn_id,
                "qos": dict(qos),
            },
            {
                "ue_id": ue_id,
                "session_id": sid,
                "direction": "DL",
                "next_hop": self.ran_id,
                "qos": dict(qos),
            },
        ]

    def _push_upf_rules(self, ue_id: str, sid: int, qos: dict, *, trace_id: str = "") -> None:
        if self.rule_plan_fn is not None:
            for upf_eid, rules in self.rule_plan_fn(self, ue_id, sid, qos):
                self._install_rules_on_upf(upf_eid, rules, trace_id=trace_id)
            return
        rules = self._build_upf_rules(ue_id, sid, qos)
        self._install_rules_on_upf(self.upf_id, rules, trace_id=trace_id)

    def _install_rules_on_upf(self, upf_eid: str, rules: list[dict], *, trace_id: str = "") -> None:
        if not self.send_upf_rule_install_packet:
            if self.sim is None:
                return
            ent = self.sim.entities.get(upf_eid)
            if isinstance(ent, UPF):
                ent.apply_install_rules(rules)
            return
        upf_pkt = Packet.control_signal(
            "UPF_INSTALL_RULES",
            src=self.eid,
            dst=upf_eid,
            payload={"rules": rules},
            creation_time=self.sim.time if self.sim else 0.0,
            trace_id=trace_id,
        )
        self.send_packet(upf_eid, upf_pkt)

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if pkt.plane != PacketPlane.CONTROL:
            return
        if pkt.msg_type == "SMF_CREATE_SESSION":
            ue_id = pkt.control_payload["ue_id"]
            sid = pkt.control_payload["session_id"]
            pol_req = Packet.control_signal(
                "PCF_POLICY_REQUEST",
                src=self.eid,
                dst=self.pcf_id,
                payload={"ue_id": ue_id, "session_id": sid},
                creation_time=self.sim.time if self.sim else 0.0,
                trace_id=pkt.trace_id,
            )
            self.send_packet(self.pcf_id, pol_req)
            self.sessions[(ue_id, sid)] = {"state": "PENDING"}
        elif pkt.msg_type == "PCF_POLICY_RESPONSE":
            ue_id = pkt.control_payload["ue_id"]
            sid = pkt.control_payload["session_id"]
            qos = dict(pkt.control_payload.get("qos", {"qfi": 1, "priority": 1}))
            self.sessions[(ue_id, sid)] = {"state": "ACTIVE", "qos": qos}
            self._push_upf_rules(ue_id, sid, qos, trace_id=pkt.trace_id)
            amf_id = self._amf_neighbor()
            if amf_id:
                resp = Packet.control_signal(
                    "PDU_SESSION_ESTABLISHMENT_ACCEPT",
                    src=self.eid,
                    dst=ue_id,
                    payload={"session_id": sid, "qos": qos},
                    creation_time=self.sim.time if self.sim else 0.0,
                    trace_id=pkt.trace_id,
                )
                self.send_packet(amf_id, resp)
        elif pkt.msg_type == "SMF_MODIFY_SESSION":
            ue_id = pkt.control_payload["ue_id"]
            sid = int(pkt.control_payload["session_id"])
            key = (ue_id, sid)
            sess = self.sessions.get(key)
            if not sess or sess.get("state") != "ACTIVE":
                return
            qos = dict(sess.get("qos", {}))
            qos.update(pkt.control_payload.get("qos", {}))
            sess["qos"] = qos
            self._push_upf_rules(ue_id, sid, qos, trace_id=pkt.trace_id)
            amf_id = self._amf_neighbor()
            if amf_id:
                notify = Packet.control_signal(
                    "PDU_SESSION_QOS_NOTIFY",
                    src=self.eid,
                    dst=ue_id,
                    payload={"session_id": sid, "qos": dict(qos)},
                    creation_time=self.sim.time if self.sim else 0.0,
                    trace_id=pkt.trace_id,
                )
                self.send_packet(amf_id, notify)
        elif pkt.msg_type == "AI_QOS_REOPT_REQUEST":
            dst = self.reopt_dst
            if dst:
                fwd = Packet.control_signal(
                    "AI_QOS_REOPT_REQUEST",
                    src=self.eid,
                    dst=dst,
                    payload=dict(pkt.control_payload),
                    creation_time=self.sim.time if self.sim else 0.0,
                    trace_id=pkt.trace_id,
                )
                self.send_packet(dst, fwd)


class PCF(Entity):
    def __init__(
        self,
        eid: str,
        *,
        upf_qos_class_by_ue_id: dict[str, int] | None = None,
        node_service_s: float | None = None,
        queue_capacity: int | None = None,
    ) -> None:
        ns = NODE_SERVICE_AMF_SMF_S if node_service_s is None else float(node_service_s)
        super().__init__(eid, node_service_s=ns, queue_capacity=queue_capacity)
        self._upf_qos_by_ue_id: dict[str, int] = dict(upf_qos_class_by_ue_id or {})

    def handle_event(self, event: Event) -> None:
        if event.event_type == "PACKET_ARRIVAL":
            self._accept_ingress_packet(event.payload)
            return
        if (
            event.event_type == "TIMER"
            and isinstance(event.payload, dict)
            and event.payload.get("type") == NODE_SERVICE_DONE
        ):
            self._complete_node_service(event.payload["packet"])
            return

    def _route_packet_after_node(self, pkt: Packet) -> None:
        if pkt.plane != PacketPlane.CONTROL:
            return
        if pkt.msg_type == "PCF_POLICY_REQUEST":
            ue_id = pkt.control_payload["ue_id"]
            sid = int(pkt.control_payload["session_id"])
            base_qos = {"qfi": 1, "priority": 1}
            merged_qos = dict(base_qos)
            if ue_id in self._upf_qos_by_ue_id:
                merged_qos["upf_qos_class"] = int(self._upf_qos_by_ue_id[ue_id])
            resp = Packet.control_signal(
                "PCF_POLICY_RESPONSE",
                src=self.eid,
                dst=pkt.src,
                payload={
                    "ue_id": ue_id,
                    "session_id": sid,
                    "qos": merged_qos,
                },
                creation_time=self.sim.time if self.sim else 0.0,
                trace_id=pkt.trace_id,
            )
            self.send_packet(pkt.src, resp)
