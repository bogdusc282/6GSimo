"""Event-driven control-plane triggers (scenario S2).

When ``S2_EVENT_DRIVEN_CP`` is enabled, after each discrete event the simulator may
submit an additional CP request (same path as periodic dummy heartbeats) if queue
backlogs exceed env-configured thresholds and the minimum inter-decision gap has
elapsed."""

from __future__ import annotations

import os
from collections import deque
from typing import Any

from sixg_sim.control_plane import CPRequest

CP_UPF_QUEUE_OVERLOAD = "CP_UPF_QUEUE_OVERLOAD"


def append_hybrid_overload_timestamp(sim: Any) -> None:
    """Record overload fire times for Hybrid AI burst detection (Scenario S4)."""

    dq = getattr(sim, "_hybrid_overload_recent_t", None)
    if dq is None:
        dq = deque()
        setattr(sim, "_hybrid_overload_recent_t", dq)
    dq.append(float(sim.time))
def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def s2_event_driven_cp_enabled() -> bool:
    return _truthy_env("S2_EVENT_DRIVEN_CP")


def _ran_backlog(ran: object) -> int:
    ing = getattr(ran, "_ingress", None)
    q = len(ing) if ing is not None else 0
    busy = 1 if getattr(ran, "_node_busy", False) else 0
    return q + busy


def _upf_total_backlog(upf: object) -> int:
    pri = getattr(upf, "_ingress_priority", None)
    be = getattr(upf, "_ingress_best_effort", None)
    p = len(pri) if pri is not None else 0
    b = len(be) if be is not None else 0
    busy = 1 if getattr(upf, "_node_busy", False) else 0
    return p + b + busy


def _should_trigger_s2(sim: Any, t_now: float) -> bool:
    min_gap = max(0.0, _env_float("S2_MIN_INTER_DECISION_S", 30.0))
    last = float(getattr(sim, "_s2_event_cp_last_t", -1e99))
    if t_now - last < min_gap:
        return False

    from sixg_sim.entities import RAN, UPF

    gnb_depth = max(1, _env_int("S2_GNB_QUEUE_DEPTH_TRIGGER", 7))
    upf_thr = max(1, _env_int("S2_UPF_QUEUE_THRESHOLD", 200))

    for ent in sim.entities.values():
        if isinstance(ent, RAN) and _ran_backlog(ent) >= gnb_depth:
            return True
    for ent in sim.entities.values():
        if isinstance(ent, UPF) and _upf_total_backlog(ent) >= upf_thr:
            return True
    return False


def overload_maybe_trigger_cp(sim: Any) -> None:
    """Scenario S3: when any UPF waiting depth exceeds ``sim._upf_overload_queue_threshold``, degrade UPF service and submit CP (same AMF path as S2)."""
    thr = getattr(sim, "_upf_overload_queue_threshold", None)
    if thr is None:
        return
    min_gap = max(0.0, float(getattr(sim, "_upf_overload_cp_min_gap_s", 30.0)))
    last = float(getattr(sim, "_overload_cp_last_t", -1e99))
    t_now = float(sim.time)
    if t_now - last < min_gap:
        return

    from sixg_sim.entities import UPF

    trigger = False
    for ent in sim.entities.values():
        if isinstance(ent, UPF) and ent._waiting_depth() > int(thr):
            trigger = True
            break
    if not trigger:
        return

    for ent in sim.entities.values():
        if isinstance(ent, UPF):
            ent.enter_overload_degraded()

    from sixg_sim.entities import AMF

    for ent in sim.entities.values():
        if not isinstance(ent, AMF):
            continue
        cp = ent.control_plane
        if cp is None or getattr(ent, "skip_control_plane_dispatch", False):
            continue
        cp.process_request(
            CPRequest(ue_id="*", slice_id="*", event_type=CP_UPF_QUEUE_OVERLOAD, kpis={}),
            ent,
            original_packet=None,
        )
        setattr(sim, "_overload_cp_last_t", t_now)
        append_hybrid_overload_timestamp(sim)
        if getattr(sim, "_track_upf_super_metrics", False):
            sim.mark_upf_overload_episode_start(t_now)
        return


def s2_maybe_trigger_cp(sim: Any) -> None:
    """If S2 is enabled and conditions hold, enqueue one CP request via the first AMF."""

    if not s2_event_driven_cp_enabled():
        return
    t_now = float(sim.time)
    if not _should_trigger_s2(sim, t_now):
        return

    from sixg_sim.entities import AMF

    for ent in sim.entities.values():
        if not isinstance(ent, AMF):
            continue
        cp = ent.control_plane
        if cp is None or getattr(ent, "skip_control_plane_dispatch", False):
            continue
        cp.process_request(
            CPRequest(ue_id="*", slice_id="*", event_type="CP_EVENT_DRIVEN_TRIGGER", kpis={}),
            ent,
            original_packet=None,
        )
        setattr(sim, "_s2_event_cp_last_t", t_now)
        return
