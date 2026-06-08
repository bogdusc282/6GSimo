from __future__ import annotations

"""Discrete-event simulation engine for packet-level 6G experiments.

`Simulation` holds the current simulated time and a priority queue of `Event`
objects. The main loop dispatches ``handle_event()`` on the target entity.

All signalling and user data use a unified :class:`Packet`. :class:`Link` only
schedules ``PACKET_ARRIVAL`` on the receiver after propagation latency. Each
:class:`Entity` has an ingress FIFO and a single server (configurable service
time); after service, :meth:`Entity._route_packet_after_node` implements NF
forwarding logic.
"""

import csv
import heapq
import math
import random
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sixg_sim.core import Event, Packet, PacketPlane

if TYPE_CHECKING:
    pass

NODE_SERVICE_DONE = "NODE_SERVICE_DONE"


def _plane_for_event_type(event_type: str, payload: Any = None) -> str:
    if event_type == "PACKET_ARRIVAL" and isinstance(payload, Packet):
        return "control" if payload.plane == PacketPlane.CONTROL else "user"
    return "internal"


def _format_event_detail(event_type: str, payload: Any, *, trace_id: str = "") -> str:
    tid = f"[{trace_id}] " if trace_id else ""
    if event_type == "PACKET_ARRIVAL" and isinstance(payload, Packet):
        p = payload
        if p.plane == PacketPlane.CONTROL:
            keys = list(p.control_payload.keys())
            keys_s = ",".join(keys[:6]) + ("…" if len(keys) > 6 else "")
            return f"{tid}{p.msg_type} {p.src}→{p.dst} payload[{keys_s}]"
        return (
            f"{tid}{p.src}→{p.dst} {p.size_bytes}B {p.traffic_type.name} "
            f"session={p.session_id} ue={p.ue_id} qfi={p.qos_flow_id}"
        )
    if event_type == "TIMER" and isinstance(payload, dict):
        return str(payload)
    if payload is None:
        return ""
    return repr(payload)[:200]


def group_packet_lifecycle(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group lifecycle log rows by ``trace_id`` (chronological order preserved per id)."""
    out: dict[str, list[dict[str, Any]]] = {}
    for row in entries:
        tid = str(row.get("trace_id", ""))
        if not tid:
            continue
        out.setdefault(tid, []).append(row)
    return out


def packet_lifecycle_span_records(lifecycle_log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per traced packet: span from first to last lifecycle timestamp (simulation seconds)."""
    by_tid: dict[str, list[tuple[float, str]]] = {}
    for row in lifecycle_log:
        tid = str(row.get("trace_id", "")).strip()
        if not tid:
            continue
        t = float(row["time_s"])
        plane = str(row.get("plane", ""))
        by_tid.setdefault(tid, []).append((t, plane))
    out: list[dict[str, Any]] = []
    for tid, events in sorted(by_tid.items(), key=lambda x: x[0]):
        times = [e[0] for e in events]
        span = max(times) - min(times) if times else 0.0
        out.append(
            {
                "trace_id": tid,
                "span_s": span,
                "plane": events[0][1] if events else "",
                "n_events": len(events),
            }
        )
    return out


def packet_arrival_span_records(event_log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Approximate per-packet span from first to last ``PACKET_ARRIVAL`` with the same ``trace_id``."""
    by_tid: dict[str, list[tuple[float, str]]] = {}
    for row in event_log:
        if row.get("event_type") != "PACKET_ARRIVAL":
            continue
        tid = str(row.get("trace_id", "")).strip()
        if not tid:
            continue
        t = float(row["time_s"])
        plane = str(row.get("plane", ""))
        by_tid.setdefault(tid, []).append((t, plane))
    out: list[dict[str, Any]] = []
    for tid, events in sorted(by_tid.items(), key=lambda x: x[0]):
        times = [e[0] for e in events]
        span = max(times) - min(times) if times else 0.0
        out.append(
            {
                "trace_id": tid,
                "span_s": span,
                "plane": events[0][1] if events else "",
                "n_arrivals": len(events),
            }
        )
    return out


def write_packet_lifecycle_csv(path: str | Path, sim: Simulation) -> Path:
    """Persist :attr:`Simulation.packet_lifecycle_log` as UTF-8 CSV (creates parent directories)."""
    outfile = Path(path)
    outfile.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "time_s",
        "trace_id",
        "phase",
        "at_entity",
        "peer",
        "plane",
        "summary",
        "detail",
        "line",
        "queue_depth",
        "queue_capacity",
        "latency_upf_s",
    ]
    with outfile.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sim.packet_lifecycle_log:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    return outfile.resolve()


def entity_queue_statistics(sim: Simulation) -> dict[str, dict[str, Any]]:
    """Per-entity ingress drop counters, capacity, and queue occupancy metrics for summary export."""
    out: dict[str, dict[str, Any]] = {}
    t_end = float(sim.time)
    for eid, ent in sim.entities.items():
        snap = getattr(ent, "queue_statistics_snapshot", None)
        if callable(snap):
            out[eid] = snap(t_end)
        else:
            out[eid] = {
                "drop_count": int(getattr(ent, "drop_count", 0)),
                "queue_capacity": getattr(ent, "queue_capacity", None),
            }
    return out


def upf_experiment_summary(sim: Simulation) -> dict[str, Any]:
    """Aggregate UPF latency samples (user-plane service sojourns) and UPF drop counts."""
    from sixg_sim.entities import UPF

    samples = sorted(float(x) for x in getattr(sim, "upf_latency_samples", []) if x == x)
    n = len(samples)
    avg = sum(samples) / n if n else 0.0
    p99 = samples[min(n - 1, max(0, int(math.ceil(0.99 * n)) - 1))] if n else 0.0
    drops = 0
    for ent in sim.entities.values():
        if isinstance(ent, UPF):
            drops += int(getattr(ent, "drop_count", 0))
    return {
        "avg_upf_latency_s": float(avg),
        "p99_upf_latency_s": float(p99),
        "upf_drop_count": int(drops),
        "upf_latency_sample_count": int(n),
    }


def upf_super_metrics_summary(sim: Simulation) -> dict[str, Any]:
    """Extra keys for S3 super: degrade episodes, ∫backlog·dt while UPF degraded, conditional UPF delay."""
    if not getattr(sim, "_track_upf_super_metrics", False):
        return {}
    from sixg_sim.entities import UPF

    ints: list[tuple[float, float]] = list(getattr(sim, "upf_deg_episode_intervals", []))
    pend = getattr(sim, "_upf_deg_pending_t0", None)
    wall_closed = sum(max(0.0, tb - ta) for ta, tb in ints)
    tail = 0.0
    if pend is not None:
        tail = max(0.0, float(sim.time) - float(pend))
    wall = wall_closed + tail

    samples = sorted(
        float(x) for x in getattr(sim, "upf_latency_samples_deg_episode", []) if x == x and x >= 0.0
    )
    ne = len(samples)
    avg_e = sum(samples) / ne if ne else 0.0
    p99_e = (
        samples[min(ne - 1, max(0, int(math.ceil(0.99 * ne)) - 1))] if ne else 0.0
    )
    degraded_now = any(
        isinstance(ent, UPF) and getattr(ent, "is_overloaded", False) for ent in sim.entities.values()
    )
    return {
        "upf_deg_episodes_completed": len(ints),
        "upf_deg_episode_pending_at_stop": degraded_now or pend is not None,
        "upf_deg_wall_time_s": float(wall),
        "upf_deg_backlog_integral_packets_s": float(
            getattr(sim, "_upf_deg_backlog_time_integral", 0.0)
        ),
        "avg_upf_latency_in_cp_deg_episode_s": float(avg_e),
        "p99_upf_latency_in_cp_deg_episode_s": float(p99_e),
        "upf_latency_in_cp_deg_episode_sample_count": int(ne),
    }


class Simulation:
    def __init__(
        self,
        *,
        event_logging: bool = True,
        event_log_max_entries: int = 50_000,
        packet_tracing: bool | None = None,
        packet_lifecycle_max_entries: int = 500_000,
    ) -> None:
        self.time = 0.0
        self.event_queue: list[Event] = []
        self.entities: dict[str, Entity] = {}
        self._event_seq = 0
        self._packet_seq = 0
        self.event_logging = event_logging
        self.event_log_max_entries = event_log_max_entries
        self.event_log: list[dict[str, Any]] = []
        self.event_log_capped: bool = False
        self.packet_tracing = event_logging if packet_tracing is None else packet_tracing
        self.packet_lifecycle_max_entries = int(packet_lifecycle_max_entries)
        self.packet_lifecycle_log: list[dict[str, Any]] = []
        self.packet_lifecycle_capped: bool = False
        #: Completed user-plane UPF sojourns (ingress → end of UPF service) for summary export.
        self.upf_latency_samples: list[float] = []

        #: S3 “super'' metrics — see :func:`upf_super_metrics_summary` (default off until modular enables).
        self._track_upf_super_metrics: bool = False
        #: Closed overload→CP‑reconfigure wall-clock intervals ``(t_overload_trigger, t_reconfigure)``.
        self.upf_deg_episode_intervals: list[tuple[float, float]] = []
        #: PDU UPF latencies whose UPF ingress fell inside a degrade episode interval.
        self.upf_latency_samples_deg_episode: list[float] = []
        self._upf_deg_pending_t0: float | None = None
        self._upf_deg_backlog_time_integral: float = 0.0

    def _ensure_trace_id(self, pkt: Packet) -> str:
        if pkt.trace_id:
            return pkt.trace_id
        self._packet_seq += 1
        pkt.trace_id = f"p{self._packet_seq}"
        return pkt.trace_id

    def trace_packet(
        self,
        phase: str,
        pkt: Packet,
        *,
        at_entity: str,
        peer: str = "",
        detail: str = "",
    ) -> None:
        """Append one lifecycle row for *pkt* (assigns ``trace_id`` on first use)."""
        if not self.packet_tracing:
            return
        if len(self.packet_lifecycle_log) >= self.packet_lifecycle_max_entries:
            self.packet_lifecycle_capped = True
            return
        tid = self._ensure_trace_id(pkt)
        plane = "control" if pkt.plane == PacketPlane.CONTROL else "user"
        summary = (
            f"{pkt.msg_type} {pkt.src}→{pkt.dst}"
            if pkt.plane == PacketPlane.CONTROL
            else f"UE {pkt.ue_id} {pkt.size_bytes}B sess={pkt.session_id}"
        )
        line = f"{self.time:.6f} [{tid}] {phase} @ {at_entity}"
        if peer:
            line += f" peer={peer}"
        if detail:
            line += f" | {detail}"
        row: dict[str, Any] = {
            "time_s": self.time,
            "trace_id": tid,
            "phase": phase,
            "at_entity": at_entity,
            "peer": peer,
            "plane": plane,
            "summary": summary,
            "detail": detail,
            "line": line,
            "queue_depth": "",
            "queue_capacity": "",
            "latency_upf_s": "",
        }
        self.packet_lifecycle_log.append(row)

    def trace_queue_depth(
        self,
        *,
        at_entity: str,
        queue_depth: int,
        queue_capacity: int,
        detail: str = "",
    ) -> None:
        """Append a QUEUE_DEPTH row (no packet trace_id; queue occupancy only)."""
        if not self.packet_tracing:
            return
        if len(self.packet_lifecycle_log) >= self.packet_lifecycle_max_entries:
            self.packet_lifecycle_capped = True
            return
        line = f"{self.time:.6f} [queue] QUEUE_DEPTH @ {at_entity} depth={queue_depth}/{queue_capacity}"
        if detail:
            line += f" | {detail}"
        row: dict[str, Any] = {
            "time_s": self.time,
            "trace_id": "",
            "phase": "QUEUE_DEPTH",
            "at_entity": at_entity,
            "peer": "",
            "plane": "queue",
            "summary": "-",
            "detail": detail,
            "line": line,
            "queue_depth": queue_depth,
            "queue_capacity": queue_capacity,
            "latency_upf_s": "",
        }
        self.packet_lifecycle_log.append(row)

    def trace_upf_latency(
        self,
        pkt: Packet,
        *,
        latency_upf_s: float,
        at_entity: str,
        detail: str = "",
    ) -> None:
        """Append ``UPF_LATENCY`` lifecycle row and record a sample for :func:`upf_experiment_summary`."""
        if not self.packet_tracing:
            return
        if len(self.packet_lifecycle_log) >= self.packet_lifecycle_max_entries:
            self.packet_lifecycle_capped = True
            return
        tid = self._ensure_trace_id(pkt)
        plane = "control" if pkt.plane == PacketPlane.CONTROL else "user"
        summary = (
            f"{pkt.msg_type} {pkt.src}→{pkt.dst}"
            if pkt.plane == PacketPlane.CONTROL
            else f"UE {pkt.ue_id} {pkt.size_bytes}B sess={pkt.session_id}"
        )
        lat = float(latency_upf_s)
        line = f"{self.time:.6f} [{tid}] UPF_LATENCY lat_upf={lat:.9g}s @ {at_entity}"
        if detail:
            line += f" | {detail}"
        row: dict[str, Any] = {
            "time_s": self.time,
            "trace_id": tid,
            "phase": "UPF_LATENCY",
            "at_entity": at_entity,
            "peer": "",
            "plane": plane,
            "summary": summary,
            "detail": detail,
            "line": line,
            "queue_depth": "",
            "queue_capacity": "",
            "latency_upf_s": lat,
        }
        self.packet_lifecycle_log.append(row)
        if pkt.plane == PacketPlane.USER:
            self.upf_latency_samples.append(lat)
            if self._track_upf_super_metrics and self._upf_user_ingress_time_in_deg_episode(
                pkt.upf_ingress_time
            ):
                self.upf_latency_samples_deg_episode.append(lat)

    def mark_upf_overload_episode_start(self, t_overload_trigger: float) -> None:
        """Overload hook (S3): open an episode bracket at UPF degraded trigger time."""
        if not self._track_upf_super_metrics:
            return
        if self._upf_deg_pending_t0 is None:
            self._upf_deg_pending_t0 = float(t_overload_trigger)

    def mark_upf_cp_reconfigure_completed(self) -> None:
        """CP hook: close the open overload episode after ``apply_reconfigure_upf``."""
        if not self._track_upf_super_metrics:
            return
        t0 = self._upf_deg_pending_t0
        if t0 is None:
            return
        t1 = float(self.time)
        self.upf_deg_episode_intervals.append((float(t0), t1))
        self._upf_deg_pending_t0 = None

    def _accumulate_upf_deg_backlog_integral(self, t_prev: float, t_next: float) -> None:
        """Piecewise ∫ (Σ UPF backlog) dt while any UPF is overload-degraded; call on each event gap."""
        if t_next <= t_prev:
            return
        from sixg_sim.entities import UPF

        if not any(
            isinstance(ent, UPF) and getattr(ent, "is_overloaded", False) for ent in self.entities.values()
        ):
            return
        backlog_sum = 0
        for ent in self.entities.values():
            if isinstance(ent, UPF):
                backlog_sum += ent._waiting_depth()
                backlog_sum += 1 if getattr(ent, "_node_busy", False) else 0
        self._upf_deg_backlog_time_integral += float(backlog_sum) * (float(t_next) - float(t_prev))

    def _upf_user_ingress_time_in_deg_episode(self, ingress_time: float | None) -> bool:
        if ingress_time is None:
            return False
        t = float(ingress_time)
        for t0, t1 in self.upf_deg_episode_intervals:
            if t0 <= t <= t1:
                return True
        pend = self._upf_deg_pending_t0
        if pend is not None and t >= pend:
            from sixg_sim.entities import UPF

            if any(
                isinstance(ent, UPF) and getattr(ent, "is_overloaded", False)
                for ent in self.entities.values()
            ):
                return True
        return False

    def register_entity(self, entity: Entity) -> None:
        self.entities[entity.eid] = entity
        entity.sim = self

    def schedule_event(self, event: Event) -> None:
        heapq.heappush(self.event_queue, event)

    def schedule(
        self,
        *,
        time: float,
        priority: int,
        target: Entity,
        event_type: str,
        payload: object | None = None,
    ) -> None:
        self._event_seq += 1
        ev = Event(
            time=time,
            priority=priority,
            seq=self._event_seq,
            target=target,
            event_type=event_type,
            payload=payload,
        )
        self.schedule_event(ev)

    def _append_event_log(self, event: Event) -> None:
        if not self.event_logging:
            return
        if len(self.event_log) >= self.event_log_max_entries:
            self.event_log_capped = True
            return
        tgt = getattr(event.target, "eid", type(event.target).__name__)
        plane = _plane_for_event_type(event.event_type, event.payload)
        trace_id = ""
        if event.event_type == "PACKET_ARRIVAL" and isinstance(event.payload, Packet):
            sim = getattr(event.target, "sim", None)
            if sim is not None:
                trace_id = sim._ensure_trace_id(event.payload)
        detail = _format_event_detail(event.event_type, event.payload, trace_id=trace_id)
        line = f"{event.time:.6f} [{plane}] @ {tgt} | {event.event_type}: {detail}"
        row: dict[str, Any] = {
            "time_s": event.time,
            "plane": plane,
            "target": tgt,
            "event_type": event.event_type,
            "detail": detail,
            "line": line,
        }
        if trace_id:
            row["trace_id"] = trace_id
        self.event_log.append(row)

    def run(self, until: float = float("inf")) -> None:
        from sixg_sim.entities import AMF
        from sixg_sim.s2_event_driven import overload_maybe_trigger_cp, s2_event_driven_cp_enabled, s2_maybe_trigger_cp

        iv = getattr(self, "_cp_strategic_dummy_interval_s", None)
        if (
            iv is not None
            and float(iv) > 0.0
            and not getattr(self, "_cp_strategic_dummy_armed", False)
            and getattr(self, "entities", None)
        ):
            setattr(self, "_cp_strategic_dummy_armed", True)
            ut = float(until) if math.isfinite(float(until)) else 1e18
            for ent in self.entities.values():
                if not isinstance(ent, AMF):
                    continue
                if getattr(ent, "skip_control_plane_dispatch", False):
                    continue
                cp = getattr(ent, "control_plane", None)
                if cp is None:
                    continue
                ent.schedule_cp_dummy_triggers(float(iv), ut)

        s2_on = s2_event_driven_cp_enabled()
        self.events_processed = 0
        while self.event_queue and self.time <= until:
            self.events_processed += 1
            event = heapq.heappop(self.event_queue)
            t_prev = float(self.time)
            t_next = float(event.time)
            if getattr(self, "_track_upf_super_metrics", False):
                self._accumulate_upf_deg_backlog_integral(t_prev, t_next)
            self.time = t_next
            self._append_event_log(event)
            event.target.handle_event(event)
            if s2_on:
                s2_maybe_trigger_cp(self)
            overload_maybe_trigger_cp(self)


class Entity:
    """Network function with ingress FIFO + single non-preemptive server.

    Service times are exponential with mean ``node_service_s`` (M/M/1-style), except when
    ``node_service_s <= 0`` (immediate pass-through). Subclasses may override
    :meth:`_node_service_time_for` (e.g. :class:`~sixg_sim.entities.UPF`).
    """

    def __init__(self, eid: str, *, node_service_s: float = 1e-5, queue_capacity: int | None = None) -> None:
        self.eid = eid
        self.node_service_s = float(node_service_s)
        if queue_capacity is not None:
            if not isinstance(queue_capacity, int) or queue_capacity < 1:
                raise ValueError("queue_capacity must be None or an integer >= 1")
        self.queue_capacity = queue_capacity
        self.drop_count = 0
        self._ingress: deque[Packet] = deque()
        self._node_busy = False
        self.sim: Simulation | None = None
        self.links: dict[str, Link] = {}
        if queue_capacity is not None:
            self._qstat_last_t = 0.0
            self._qstat_prev_depth = 0
            self._qstat_integral = 0.0
            self._qstat_max_depth = 0
            self._qstat_sample_sum = 0
            self._qstat_sample_n = 0

    def _waiting_depth(self) -> int:
        """Packets waiting for service (excludes packet currently in service)."""
        return len(self._ingress)

    def _queue_integrate_pending_interval(self) -> None:
        if self.queue_capacity is None or self.sim is None:
            return
        t = self.sim.time
        dt = t - self._qstat_last_t
        if dt > 0:
            self._qstat_integral += self._qstat_prev_depth * dt
        self._qstat_last_t = t

    def _queue_stats_after_mutation(self, reason: str) -> None:
        """Call after enqueue or dequeue changed waiting depth."""
        if self.queue_capacity is None or self.sim is None:
            return
        self._queue_integrate_pending_interval()
        d = self._waiting_depth()
        self._qstat_prev_depth = d
        self._qstat_max_depth = max(self._qstat_max_depth, d)
        self._qstat_sample_sum += d
        self._qstat_sample_n += 1
        self.sim.trace_queue_depth(
            at_entity=self.eid,
            queue_depth=d,
            queue_capacity=int(self.queue_capacity),
            detail=reason,
        )

    def _queue_stats_on_drop(self) -> None:
        """Waiting depth unchanged; integrate interval and log correlated QUEUE_DEPTH after DROP."""
        if self.queue_capacity is None or self.sim is None:
            return
        self._queue_integrate_pending_interval()
        d = self._waiting_depth()
        self._qstat_max_depth = max(self._qstat_max_depth, d)
        self._qstat_sample_sum += d
        self._qstat_sample_n += 1
        self.sim.trace_queue_depth(
            at_entity=self.eid,
            queue_depth=d,
            queue_capacity=int(self.queue_capacity),
            detail="drop_rejected",
        )

    def _finalize_queue_integral(self, t_end: float) -> None:
        if self.queue_capacity is None:
            return
        dt = t_end - self._qstat_last_t
        if dt > 0:
            self._qstat_integral += self._qstat_prev_depth * dt
        self._qstat_last_t = t_end

    def queue_statistics_snapshot(self, t_end: float) -> dict[str, Any]:
        qc = self.queue_capacity
        dc = int(self.drop_count)
        if qc is None:
            return {"drop_count": dc, "queue_capacity": None}
        self._finalize_queue_integral(t_end)
        tw = self._qstat_integral / t_end if t_end > 1e-18 else 0.0
        avg_s = self._qstat_sample_sum / self._qstat_sample_n if self._qstat_sample_n else 0.0
        return {
            "drop_count": dc,
            "queue_capacity": int(qc),
            "max_queue_depth": int(self._qstat_max_depth),
            "avg_queue_depth": float(avg_s),
            "time_weighted_avg_queue_depth": float(tw),
        }

    def connect(self, other: Entity, link: Link) -> None:
        self.links[other.eid] = link

    def send_packet(self, dst_id: str, packet: Packet) -> None:
        if self.sim is None:
            raise RuntimeError(f"{self.eid} is not registered with a Simulation")
        link = self.links[dst_id]
        dst = self.sim.entities[dst_id]
        link.send_packet(self, dst, packet)

    def _accept_ingress_packet(self, pkt: Packet) -> None:
        qc = self.queue_capacity
        if qc is not None and len(self._ingress) >= qc:
            self.drop_count += 1
            if self.sim is not None:
                self.sim.trace_packet(
                    "DROP",
                    pkt,
                    at_entity=self.eid,
                    detail=f"reason=queue_full queue_capacity={qc} queue_depth_waiting={len(self._ingress)}",
                )
                self._queue_stats_on_drop()
            return
        self._ingress.append(pkt)
        self._queue_stats_after_mutation("enqueue")
        if self.sim is not None:
            self.sim.trace_packet(
                "INGRESS",
                pkt,
                at_entity=self.eid,
                detail=f"queue_depth_after={len(self._ingress)}",
            )
        self._drain_node_server()

    def _drain_node_server(self) -> None:
        if self._node_busy or not self._ingress or self.sim is None:
            return
        if self.node_service_s <= 0.0:
            pkt = self._ingress.popleft()
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
        pkt = self._ingress.popleft()
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

    def _node_service_time_for(self, pkt: Packet) -> float:
        mean_s = float(self.node_service_s)
        if mean_s <= 0.0:
            return 0.0
        return random.expovariate(1.0 / mean_s)

    def _complete_node_service(self, pkt: Packet) -> None:
        if self.sim is not None:
            self.sim.trace_packet("NODE_END", pkt, at_entity=self.eid)
        self._node_busy = False
        self._route_packet_after_node(pkt)
        self._drain_node_server()

    def _route_packet_after_node(self, pkt: Packet) -> None:
        raise NotImplementedError(f"{type(self).__name__} must implement _route_packet_after_node")

    def handle_event(self, event: Event) -> None:
        raise NotImplementedError


class Link:
    def __init__(self, latency: float) -> None:
        self.latency = latency

    def send_packet(self, src: Entity, dst: Entity, packet: Packet) -> None:
        if src.sim is None:
            raise RuntimeError("source entity has no simulation")
        arrival_time = src.sim.time + self.latency
        pri = 0 if packet.plane == PacketPlane.CONTROL else 1
        src.sim.trace_packet(
            "LINK_TX",
            packet,
            at_entity=src.eid,
            peer=dst.eid,
            detail=f"latency_s={self.latency:.6g} arrival_s={arrival_time:.6g}",
        )
        src.sim.schedule(
            time=arrival_time,
            priority=pri,
            target=dst,
            event_type="PACKET_ARRIVAL",
            payload=packet,
        )
