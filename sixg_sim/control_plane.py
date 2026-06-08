from __future__ import annotations

import os
import random
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from sixg_sim.core import Packet
from sixg_sim.simulation import Simulation

# Timer payload type (AMF must handle this).
CP_SERVICE_DONE = "CP_SERVICE_DONE"
CP_DUMMY_TICK = "CP_DUMMY_TICK"

CP_BASELINE = "CP_BASELINE"
CP_AI_SINGLE = "CP_AI_SINGLE"
CP_AI_DOUBLE = "CP_AI_DOUBLE"
CP_AI_HYBRID = "CP_AI_HYBRID"

SERVER_BASELINE = "CP_BASELINE"
SERVER_AI_SINGLE = "CP_AI_SINGLE"
SERVER_AI_HYBRID_FALLBACK = "CP_AI_HYBRID_FALLBACK"
SERVER_PLANNING = "CP_PLANNING"
SERVER_EXECUTION = "CP_EXECUTION"

BASELINE_DECISION_LATENCY_S = 0.010
AI_STAGE_LATENCY_S = 0.500

CP_UPF_QUEUE_OVERLOAD_EVT = "CP_UPF_QUEUE_OVERLOAD"


@dataclass
class CPRequest:
    ue_id: str
    slice_id: str
    event_type: str
    kpis: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> CPRequest:
        return CPRequest(
            ue_id=str(d.get("ue_id", "")),
            slice_id=str(d.get("slice_id", "")),
            event_type=str(d.get("event_type", "")),
            kpis={str(k): float(v) for k, v in (d.get("kpis") or {}).items()},
        )


@dataclass
class CPDecision:
    """Actionable result from the control-plane module (local policy / AI synthesis)."""

    admit: bool = True
    forward_to_smf: bool = True
    mode_name: str = CP_BASELINE
    reason: str = ""
    #: Optional NF action applied on :func:`deliver_cp_completion` (e.g. ``RECONFIGURE_UPF`` for S3).
    nf_action: str = ""

    def with_mode(self, mode_name: str) -> CPDecision:
        return CPDecision(
            admit=self.admit,
            forward_to_smf=self.forward_to_smf,
            mode_name=mode_name,
            reason=self.reason,
            nf_action=self.nf_action,
        )


class ControlPlaneServer:
    """FIFO single-server queue with deterministic (or sampled) service time (virtual clock).

    If ``service_time_fn`` is set, each visit uses ``service_time = service_time_fn()``; otherwise
    the fixed nominal ``service_time_s`` is used (deterministic).
    """

    def __init__(
        self,
        service_time_s: float,
        name: str,
        *,
        service_time_fn: Callable[[], float] | None = None,
        service_distribution: Literal["deterministic", "exponential"] | None = None,
    ) -> None:
        self._nominal_service_s = float(service_time_s)
        self.service_time_fn = service_time_fn
        self.name = str(name)
        if service_distribution is not None:
            self.service_distribution: Literal["deterministic", "exponential"] = service_distribution
        else:
            self.service_distribution = "deterministic" if service_time_fn is None else "exponential"
        self.next_available_time: float = 0.0
        self.total_requests: int = 0
        self.total_waiting_time_s: float = 0.0
        self.total_service_time_s: float = 0.0
        self.total_latency_time_s: float = 0.0

    @property
    def nominal_service_s(self) -> float:
        return float(self._nominal_service_s)

    def _draw_service_s(self) -> float:
        if self.service_time_fn is not None:
            return max(1e-15, float(self.service_time_fn()))
        return max(1e-15, float(self._nominal_service_s))

    def enqueue_at(self, t_now: float) -> tuple[float, float, float, float]:
        """Book one service. Returns (end_service_time, waiting_time, service_time, total_time from t_now)."""
        service_time = self._draw_service_s()
        start_service_time = max(float(t_now), float(self.next_available_time))
        waiting_time = start_service_time - float(t_now)
        end_service_time = start_service_time + service_time
        total_time = end_service_time - float(t_now)
        self.next_available_time = end_service_time
        self.total_requests += 1
        self.total_waiting_time_s += waiting_time
        self.total_service_time_s += service_time
        self.total_latency_time_s += total_time
        return end_service_time, waiting_time, service_time, total_time

    def summary_dict(self) -> dict[str, Any]:
        n = self.total_requests
        avg_svc = (self.total_service_time_s / n) if n else 0.0
        return {
            "server_name": self.name,
            "service_distribution": self.service_distribution,
            "total_requests": n,
            "avg_waiting_time_s": (self.total_waiting_time_s / n) if n else 0.0,
            "avg_service_time_s": avg_svc,
            "avg_sampled_service_time_s": avg_svc,
            "avg_total_time_s": (self.total_latency_time_s / n) if n else 0.0,
            "total_waiting_time_s": self.total_waiting_time_s,
            "total_service_time_s": self.total_service_time_s,
            "total_latency_time_s": self.total_latency_time_s,
        }


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def _cp_decision_log_max_entries() -> int:
    """Max rows kept in ``cp_decision_log``. Unset default 1_000_000; ``<= 0`` means unlimited."""

    v = _env_int("CP_DECISION_LOG_MAX_ENTRIES", 1_000_000)
    if v <= 0:
        return 10**15
    return v


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _cp_llm_tokens_input() -> int:
    """Average input tokens per LLM inference (env ``CP_TOKENS_INPUT``, default 2000)."""

    return max(0, _env_int("CP_TOKENS_INPUT", 2000))


def _cp_llm_tokens_output() -> int:
    """Average output tokens per LLM inference (env ``CP_TOKENS_OUTPUT``, default 1000)."""

    return max(0, _env_int("CP_TOKENS_OUTPUT", 1000))


def _cp_llm_price_per_1m_input() -> float:
    return max(0.0, _env_float("CP_PRICE_PER_1M_INPUT", 5.0))


def _cp_llm_price_per_1m_output() -> float:
    return max(0.0, _env_float("CP_PRICE_PER_1M_OUTPUT", 30.0))


def _cp_llm_price_per_1m_input_cached() -> float:
    return max(0.0, _env_float("CP_PRICE_PER_1M_INPUT_CACHED", 0.5))


def llm_inference_cost_for_n_calls(n_calls: int) -> tuple[int, int, float]:
    """LLM inference cost model: token counts and cost for *n_calls* inferences (per-1M token rates)."""

    n = max(0, int(n_calls))
    tin = n * _cp_llm_tokens_input()
    tout = n * _cp_llm_tokens_output()
    cost = (tin / 1e6) * _cp_llm_price_per_1m_input() + (tout / 1e6) * _cp_llm_price_per_1m_output()
    return tin, tout, float(cost)


def _env_tokens_per_call() -> int:
    """Legacy aggregate tokens; defaults to sum of input + output from the LLM inference cost model if unset."""

    raw = os.environ.get("CONTROL_PLANE_TOKENS_PER_CALL")
    if raw is not None and str(raw).strip() != "":
        return max(1, _env_int("CONTROL_PLANE_TOKENS_PER_CALL", 3000))
    return max(1, _cp_llm_tokens_input() + _cp_llm_tokens_output())


def control_plane_ai_latency_distribution() -> Literal["deterministic", "exponential"]:
    """Env ``CONTROL_PLANE_AI_LATENCY_DIST``: ``deterministic`` (default) or ``exponential``.

    Exponential uses mean equal to the server's nominal AI stage duration (500 ms per call).
    Baseline CP (10 ms) always uses deterministic service regardless of this variable.
    """
    s = (os.environ.get("CONTROL_PLANE_AI_LATENCY_DIST") or "deterministic").strip().lower()
    if s in ("exponential", "exp"):
        return "exponential"
    return "deterministic"


def configured_tokens_per_call() -> int:
    return _env_tokens_per_call()


def make_exponential_service_time_fn(nominal_mean_s: float) -> Callable[[], float]:
    """``Exp(mean=nominal_mean_s)`` via ``random.expovariate(1.0 / mean)``."""

    mean = max(1e-15, float(nominal_mean_s))

    def _sample() -> float:
        return float(random.expovariate(1.0 / mean))

    return _sample


def sample_ai_stage_latency_s() -> float:
    """One independent sample of an AI stage duration (honours ``CONTROL_PLANE_AI_LATENCY_DIST``)."""

    if control_plane_ai_latency_distribution() == "deterministic":
        return float(AI_STAGE_LATENCY_S)
    return float(random.expovariate(1.0 / max(1e-15, float(AI_STAGE_LATENCY_S))))


def _ai_service_time_fn_for_server(nominal_s: float) -> Callable[[], float] | None:
    if control_plane_ai_latency_distribution() != "exponential":
        return None
    return make_exponential_service_time_fn(nominal_s)


class ControlPlaneBase:
    """Control plane with explicit :class:`ControlPlaneServer` queueing.

    AI modes account for an **LLM inference cost model** (input/output tokens and prices per 1M tokens,
    from environment variables). Baseline mode performs no LLM inference.
    """

    name: str = CP_BASELINE

    def __init__(self) -> None:
        self.sim: Simulation | None = None
        self.cp_decision_count: int = 0
        self.cp_total_waiting_time_s: float = 0.0
        self.cp_total_service_time_s: float = 0.0
        self.cp_total_latency_time_s: float = 0.0
        self.cp_decision_log: list[dict[str, Any]] = []
        self.ai_calls_total: int = 0
        self.tokens_used_total: int = 0
        self.total_input_tokens: int = 0
        self.total_output_tokens: int = 0
        self.total_cost: float = 0.0
        # Back-compat: same as cp_total_latency_time_s (sum of end-to-end latencies).
        self.total_cp_latency_s: float = 0.0

    def bind_sim(self, sim: Simulation) -> None:
        self.sim = sim

    def _ensure_sim(self) -> Simulation:
        if self.sim is None:
            raise RuntimeError("control plane is not bound to a Simulation")
        return self.sim

    def iter_servers(self) -> list[ControlPlaneServer]:
        return []

    def ai_latency_distribution_label(self) -> Literal["deterministic", "exponential"]:
        """Label for how AI stages are sampled (baseline is always deterministic)."""

        return "deterministic"

    def aggregate_avg_sampled_service_time_s(self) -> float:
        """Mean realized service time across all CP servers (per visit)."""
        srvs = self.iter_servers()
        den = sum(s.total_requests for s in srvs)
        if not den:
            return 0.0
        return sum(s.total_service_time_s for s in srvs) / float(den)

    def record_completed_request(
        self,
        *,
        t_arrival: float,
        cp_waiting_sum_s: float,
        cp_service_sum_s: float,
        cp_total_latency_s: float,
        ai_calls: int,
        tokens: int,
    ) -> None:
        self.cp_decision_count += 1
        self.cp_total_waiting_time_s += float(cp_waiting_sum_s)
        self.cp_total_service_time_s += float(cp_service_sum_s)
        self.cp_total_latency_time_s += float(cp_total_latency_s)
        self.total_cp_latency_s += float(cp_total_latency_s)
        self.ai_calls_total += int(ai_calls)
        ac = int(ai_calls)
        if ac > 0:
            tin, tout, cst = llm_inference_cost_for_n_calls(ac)
            self.total_input_tokens += tin
            self.total_output_tokens += tout
            self.total_cost += cst
            self.tokens_used_total += tin + tout
        else:
            self.tokens_used_total += int(tokens)
        if len(self.cp_decision_log) < _cp_decision_log_max_entries():
            self.cp_decision_log.append(
                {
                    "wait_time_s": float(cp_waiting_sum_s),
                    "service_time_s": float(cp_service_sum_s),
                    "latency_time_s": float(cp_total_latency_s),
                    "t_arrival_s": float(t_arrival),
                    "ai_calls": int(ai_calls),
                }
            )

    def build_decision(self, request: CPRequest) -> CPDecision:
        if str(request.event_type) == CP_UPF_QUEUE_OVERLOAD_EVT:
            return CPDecision(
                admit=True,
                forward_to_smf=False,
                mode_name=self.name,
                reason="upf_queue_overload",
                nf_action="RECONFIGURE_UPF",
            )
        return CPDecision(admit=True, forward_to_smf=True, mode_name=self.name, reason="ok")

    def process_request(
        self,
        request: CPRequest,
        completion_target: Any,
        *,
        original_packet: Packet | None = None,
        t_now: float | None = None,
    ) -> None:
        raise NotImplementedError

    def _schedule_completion(
        self,
        completion_target: Any,
        completion_time: float,
        *,
        request: CPRequest,
        original_packet: Packet | None,
        t_arrival: float,
        wait_sum_s: float,
        service_sum_s: float,
        total_latency_s: float,
        ai_calls: int,
        tokens: int,
    ) -> None:
        sim = self._ensure_sim()
        sim.schedule(
            time=completion_time,
            priority=1,
            target=completion_target,
            event_type="TIMER",
            payload={
                "type": CP_SERVICE_DONE,
                "cp_mode": self.name,
                "t_arrival": float(t_arrival),
                "cp_waiting_sum_s": float(wait_sum_s),
                "cp_service_sum_s": float(service_sum_s),
                "cp_total_latency_s": float(total_latency_s),
                "request": request.to_dict(),
                "original_packet": original_packet,
                "ai_calls": int(ai_calls),
                "tokens": int(tokens),
            },
        )

    def summary_dict(self) -> dict[str, Any]:
        n = self.cp_decision_count
        avg_lat = (self.cp_total_latency_time_s / n) if n else 0.0
        dist = self.ai_latency_distribution_label()
        return {
            "control_plane_mode": self.name,
            "control_plane_ai_latency_dist": dist,
            "cp_decision_count": n,
            "cp_total_waiting_time_s": self.cp_total_waiting_time_s,
            "cp_total_service_time_s": self.cp_total_service_time_s,
            "cp_total_latency_time_s": self.cp_total_latency_time_s,
            "avg_service_per_decision_s": (self.cp_total_service_time_s / n) if n else 0.0,
            "avg_sampled_service_time_s": self.aggregate_avg_sampled_service_time_s(),
            "ai_calls_total": self.ai_calls_total,
            "tokens_used_total": self.tokens_used_total,
            "input_tokens_total": self.total_input_tokens,
            "output_tokens_total": self.total_output_tokens,
            "llm_cost_total": self.total_cost,
            "llm_price_per_1m_input": _cp_llm_price_per_1m_input(),
            "llm_price_per_1m_output": _cp_llm_price_per_1m_output(),
            "llm_price_per_1m_input_cached": _cp_llm_price_per_1m_input_cached(),
            "llm_cost_per_decision": (self.total_cost / n) if n else 0.0,
            "avg_cp_waiting_s": (self.cp_total_waiting_time_s / n) if n else 0.0,
            "avg_cp_service_s": (self.cp_total_service_time_s / n) if n else 0.0,
            "avg_cp_latency_s": avg_lat,
            "cp_decision_log": list(self.cp_decision_log),
            # Legacy keys for CLI / older scripts
            "decision_count": n,
            "total_cp_latency_s": self.total_cp_latency_s,
        }


class ControlPlaneBaseline(ControlPlaneBase):
    name = CP_BASELINE

    def __init__(self) -> None:
        super().__init__()
        self.cp_server = ControlPlaneServer(
            BASELINE_DECISION_LATENCY_S,
            SERVER_BASELINE,
            service_time_fn=None,
            service_distribution="deterministic",
        )

    def iter_servers(self) -> list[ControlPlaneServer]:
        return [self.cp_server]

    def process_request(
        self,
        request: CPRequest,
        completion_target: Any,
        *,
        original_packet: Packet | None = None,
        t_now: float | None = None,
    ) -> None:
        sim = self._ensure_sim()
        t = float(sim.time if t_now is None else t_now)
        end_t, w, s, tot = self.cp_server.enqueue_at(t)
        self._schedule_completion(
            completion_target,
            end_t,
            request=request,
            original_packet=original_packet,
            t_arrival=t,
            wait_sum_s=w,
            service_sum_s=s,
            total_latency_s=tot,
            ai_calls=0,
            tokens=0,
        )


class ControlPlaneAISingle(ControlPlaneBase):
    name = CP_AI_SINGLE

    def __init__(self) -> None:
        super().__init__()
        st_fn = _ai_service_time_fn_for_server(AI_STAGE_LATENCY_S)
        dist: Literal["deterministic", "exponential"] = (
            "exponential" if st_fn is not None else "deterministic"
        )
        self.cp_server = ControlPlaneServer(
            AI_STAGE_LATENCY_S,
            SERVER_AI_SINGLE,
            service_time_fn=st_fn,
            service_distribution=dist,
        )

    def ai_latency_distribution_label(self) -> Literal["deterministic", "exponential"]:
        return control_plane_ai_latency_distribution()

    def iter_servers(self) -> list[ControlPlaneServer]:
        return [self.cp_server]

    def process_request(
        self,
        request: CPRequest,
        completion_target: Any,
        *,
        original_packet: Packet | None = None,
        t_now: float | None = None,
    ) -> None:
        sim = self._ensure_sim()
        t = float(sim.time if t_now is None else t_now)
        end_t, w, s, tot = self.cp_server.enqueue_at(t)
        self._schedule_completion(
            completion_target,
            end_t,
            request=request,
            original_packet=original_packet,
            t_arrival=t,
            wait_sum_s=w,
            service_sum_s=s,
            total_latency_s=tot,
            ai_calls=1,
            tokens=0,
        )


def _aggregate_upf_ingress_packets(sim: Simulation) -> int:
    from sixg_sim.entities import UPF

    total = 0
    for ent in sim.entities.values():
        if isinstance(ent, UPF):
            total += int(ent._waiting_depth())
            if getattr(ent, "_node_busy", False):
                total += 1
    return total


def _any_upf_is_overloaded(sim: Simulation) -> bool:
    from sixg_sim.entities import UPF

    return any(
        isinstance(ent, UPF) and getattr(ent, "is_overloaded", False) for ent in sim.entities.values()
    )


def _hybrid_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(float(raw))
    except ValueError:
        return int(default)


def _hybrid_env_float_nonneg(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return max(0.0, float(default))
    try:
        return max(0.0, float(raw))
    except ValueError:
        return max(0.0, float(default))


def _hybrid_default_ai_deadline_s() -> float:
    """Default CP SLA for Hybrid's missed-deadline stress bit vs nominal AI inference (see Scenario S4).

    Placed comfortably above ``AI_STAGE_LATENCY_S`` so an idle AI path does not violate the SLA on
    paper; override with ``S4_HYBRID_AI_DEADLINE_S``.
    """

    return float(AI_STAGE_LATENCY_S + 12.0 * BASELINE_DECISION_LATENCY_S)


def hybrid_overloads_in_window(sim: Simulation) -> int:
    """Overload fire times recorded by ``append_hybrid_overload_timestamp``."""
    dq = getattr(sim, "_hybrid_overload_recent_t", None)
    if not dq:
        return 0
    t_now = float(sim.time)
    w = _hybrid_env_float_nonneg("S4_HYBRID_BURST_WINDOW_S", 25.0)
    while dq and t_now - float(dq[0]) > w:
        dq.popleft()
    return len(dq)


class ControlPlaneHybridAI(ControlPlaneBase):
    """Hybrid AI CP (Scenario S4): baseline‑first cold start/calibration, then calm→AI‑Single when safe.

    Congestion activates deterministic baseline (~10 ms) latency‑aware fallback; see env ``S4_HYBRID_*``.
    """

    name = CP_AI_HYBRID

    def __init__(self) -> None:
        super().__init__()
        self.baseline_server = ControlPlaneServer(
            BASELINE_DECISION_LATENCY_S,
            SERVER_AI_HYBRID_FALLBACK,
            service_time_fn=None,
            service_distribution="deterministic",
        )
        st_fn = _ai_service_time_fn_for_server(AI_STAGE_LATENCY_S)
        dist: Literal["deterministic", "exponential"] = (
            "exponential" if st_fn is not None else "deterministic"
        )
        self.ai_server = ControlPlaneServer(
            AI_STAGE_LATENCY_S,
            SERVER_AI_SINGLE,
            service_time_fn=st_fn,
            service_distribution=dist,
        )
        self.hybrid_fallback_dispatch_count = 0
        self.hybrid_ai_dispatch_count = 0
        self.hybrid_calibration_baseline_dispatch_count = 0
        self._prev_agg_upf_wait: int | None = None
        self._fallback_latched: bool = False
        self._hybrid_request_serial = 0

    def ai_latency_distribution_label(self) -> Literal["deterministic", "exponential"]:
        return control_plane_ai_latency_distribution()

    def iter_servers(self) -> list[ControlPlaneServer]:
        return [self.baseline_server, self.ai_server]

    def summary_dict(self) -> dict[str, Any]:
        d = super().summary_dict()
        dist = control_plane_ai_latency_distribution()
        d["control_plane_ai_latency_dist"] = f"hybrid_{dist}"
        d["hybrid_fallback_dispatch_count"] = int(self.hybrid_fallback_dispatch_count)
        d["hybrid_ai_dispatch_count"] = int(self.hybrid_ai_dispatch_count)
        d["hybrid_calibration_baseline_dispatch_count"] = int(
            self.hybrid_calibration_baseline_dispatch_count
        )
        d["hybrid_fallback_latched"] = bool(self._fallback_latched)
        cold = max(0, _hybrid_env_int("S4_HYBRID_COLDSTART_DECISIONS", 12))
        ramp = max(0, _hybrid_env_int("S4_HYBRID_CONFIDENCE_RAMP_DECISIONS", 30))
        d["hybrid_coldstart_decisions_setting"] = int(cold)
        d["hybrid_confidence_ramp_decisions_setting"] = int(ramp)
        return d

    def process_request(
        self,
        request: CPRequest,
        completion_target: Any,
        *,
        original_packet: Packet | None = None,
        t_now: float | None = None,
    ) -> None:
        sim = self._ensure_sim()
        t = float(sim.time if t_now is None else t_now)
        et = str(request.event_type)

        agg = _aggregate_upf_ingress_packets(sim)
        overloaded = _any_upf_is_overloaded(sim)
        overload_cp = et == CP_UPF_QUEUE_OVERLOAD_EVT

        rising = False
        step = max(1, _hybrid_env_int("S4_HYBRID_RISING_DEPTH_STEP", 2))
        if self._prev_agg_upf_wait is not None:
            rising = agg >= self._prev_agg_upf_wait + step
        self._prev_agg_upf_wait = agg

        qa = max(0.0, float(self.ai_server.next_available_time - t))
        nominal_ai = AI_STAGE_LATENCY_S
        projected_ai_finish = qa + nominal_ai
        deadline = _hybrid_env_float_nonneg("S4_HYBRID_AI_DEADLINE_S", _hybrid_default_ai_deadline_s())
        missed_deadline = projected_ai_finish > deadline and not overload_cp

        q_enter = max(1, _hybrid_env_int("S4_HYBRID_QUEUE_ENTER_SUM", 10))
        q_exit = max(0, _hybrid_env_int("S4_HYBRID_QUEUE_EXIT_SUM", 4))
        congested_pressure = agg >= q_enter

        bursts = hybrid_overloads_in_window(sim)
        burst_hit = bursts >= max(2, _hybrid_env_int("S4_HYBRID_BURST_MIN_OVERLOADS", 2))
        # Bursts widen fallback only alongside current pressure — otherwise timestamps alone would
        # keep ``instant_stress`` true indefinitely after episodic overloads even when queues are idle.
        burst_stress = burst_hit and (overload_cp or overloaded or congested_pressure)

        instant_stress = overload_cp or overloaded or congested_pressure or burst_stress or rising or missed_deadline
        if instant_stress:
            self._fallback_latched = True
        elif agg <= q_exit and not overloaded:
            self._fallback_latched = False

        self._hybrid_request_serial += 1
        idx = int(self._hybrid_request_serial)
        cold_n = max(0, _hybrid_env_int("S4_HYBRID_COLDSTART_DECISIONS", 12))
        ramp_n = max(0, _hybrid_env_int("S4_HYBRID_CONFIDENCE_RAMP_DECISIONS", 30))
        # Cold start: deterministic baseline heuristic only (deployment / observability warmup).
        # Calibration ramp: when the network looks calm ("AI confidence building"), keep heuristic only.
        calibration_calm_hold = ramp_n > 0 and (cold_n < idx <= cold_n + ramp_n and not instant_stress)
        force_calibration_baseline = bool(idx <= cold_n or calibration_calm_hold)

        use_baseline = bool(force_calibration_baseline or self._fallback_latched or overload_cp)

        srv = self.baseline_server if use_baseline else self.ai_server
        if srv is self.baseline_server:
            if force_calibration_baseline:
                self.hybrid_calibration_baseline_dispatch_count += 1
            else:
                self.hybrid_fallback_dispatch_count += 1
            ac = 0
        else:
            self.hybrid_ai_dispatch_count += 1
            ac = 1

        end_t, w, s, tot = srv.enqueue_at(t)
        self._schedule_completion(
            completion_target,
            end_t,
            request=request,
            original_packet=original_packet,
            t_arrival=t,
            wait_sum_s=w,
            service_sum_s=s,
            total_latency_s=tot,
            ai_calls=ac,
            tokens=0,
        )


class ControlPlaneAIDouble(ControlPlaneBase):
    name = CP_AI_DOUBLE

    def __init__(self) -> None:
        super().__init__()
        st_fn = _ai_service_time_fn_for_server(AI_STAGE_LATENCY_S)
        dist: Literal["deterministic", "exponential"] = (
            "exponential" if st_fn is not None else "deterministic"
        )
        self.planning_server = ControlPlaneServer(
            AI_STAGE_LATENCY_S,
            SERVER_PLANNING,
            service_time_fn=st_fn,
            service_distribution=dist,
        )
        self.execute_server = ControlPlaneServer(
            AI_STAGE_LATENCY_S,
            SERVER_EXECUTION,
            service_time_fn=st_fn,
            service_distribution=dist,
        )

    def ai_latency_distribution_label(self) -> Literal["deterministic", "exponential"]:
        return control_plane_ai_latency_distribution()

    def iter_servers(self) -> list[ControlPlaneServer]:
        return [self.planning_server, self.execute_server]

    def process_request(
        self,
        request: CPRequest,
        completion_target: Any,
        *,
        original_packet: Packet | None = None,
        t_now: float | None = None,
    ) -> None:
        sim = self._ensure_sim()
        t = float(sim.time if t_now is None else t_now)
        end1, w1, s1, _t1 = self.planning_server.enqueue_at(t)
        end2, w2, s2, _t2 = self.execute_server.enqueue_at(end1)
        wait_sum = w1 + w2
        service_sum = s1 + s2
        total_lat = end2 - t
        self._schedule_completion(
            completion_target,
            end2,
            request=request,
            original_packet=original_packet,
            t_arrival=t,
            wait_sum_s=wait_sum,
            service_sum_s=service_sum,
            total_latency_s=total_lat,
            ai_calls=2,
            tokens=0,
        )


def build_control_plane_from_mode(mode: int) -> ControlPlaneBase:
    if mode == 1:
        return ControlPlaneAISingle()
    if mode == 2:
        return ControlPlaneAIDouble()
    if mode == 3:
        return ControlPlaneHybridAI()
    return ControlPlaneBaseline()


def build_control_plane_from_env() -> ControlPlaneBase:
    return build_control_plane_from_mode(_env_int("CONTROL_PLANE_MODE", 0))


def cp_request_from_packet(pkt: Packet) -> CPRequest:
    pl = pkt.control_payload if isinstance(pkt.control_payload, dict) else {}
    ue_id = str(pl.get("ue_id", ""))
    slice_id = str(pl.get("slice_id", pl.get("sst_sd", "default-slice")))
    event_type = str(pkt.msg_type or "")
    kpis: dict[str, float] = {}
    raw_kpis = pl.get("kpis")
    if isinstance(raw_kpis, dict):
        for k, v in raw_kpis.items():
            try:
                kpis[str(k)] = float(v)
            except (TypeError, ValueError):
                continue
    return CPRequest(ue_id=ue_id, slice_id=slice_id, event_type=event_type, kpis=kpis)


def bind_control_plane(sim: Simulation, cp: ControlPlaneBase) -> None:
    cp.bind_sim(sim)


def attach_default_control_plane(sim: Simulation) -> ControlPlaneBase:
    cp = build_control_plane_from_env()
    bind_control_plane(sim, cp)
    for ent in sim.entities.values():
        fn = getattr(ent, "attach_control_plane", None)
        if callable(fn):
            fn(cp)
    return cp


def ensure_default_control_plane(sim: Simulation) -> ControlPlaneBase | None:
    for ent in sim.entities.values():
        existing = getattr(ent, "control_plane", None)
        if existing is not None:
            if existing.sim is None:
                existing.bind_sim(sim)
            return existing
    has_amf = any(callable(getattr(e, "attach_control_plane", None)) for e in sim.entities.values())
    if not has_amf:
        return None
    return attach_default_control_plane(sim)


def control_plane_summary_for_sim(sim: Simulation) -> dict[str, Any] | None:
    for ent in sim.entities.values():
        cp = getattr(ent, "control_plane", None)
        if cp is not None:
            return cp.summary_dict()
    return None


def deliver_cp_completion(host: Any, payload: dict[str, Any]) -> None:
    """Final CP_SERVICE_DONE: update metrics and optionally apply NF forwarding (AMF)."""
    cp: ControlPlaneBase | None = getattr(host, "control_plane", None)
    if cp is None:
        return
    req = CPRequest.from_dict(payload["request"] if isinstance(payload.get("request"), dict) else {})
    orig = payload.get("original_packet")
    t_arrival = float(payload.get("t_arrival", 0.0))
    wait_sum = float(payload.get("cp_waiting_sum_s", 0.0))
    service_sum = float(payload.get("cp_service_sum_s", 0.0))
    total_lat = float(payload.get("cp_total_latency_s", 0.0))
    ai_calls = int(payload.get("ai_calls", 0))
    tokens = int(payload.get("tokens", 0))

    # Backward compatibility: older payloads used started_at + no queue breakdown
    if "cp_total_latency_s" not in payload and "started_at" in payload:
        t_arrival = float(payload["started_at"])
        sim = getattr(host, "sim", None)
        if sim is not None:
            total_lat = max(0.0, float(sim.time) - t_arrival)
        wait_sum = 0.0
        service_sum = total_lat

    cp.record_completed_request(
        t_arrival=t_arrival,
        cp_waiting_sum_s=wait_sum,
        cp_service_sum_s=service_sum,
        cp_total_latency_s=total_lat,
        ai_calls=ai_calls,
        tokens=tokens,
    )
    decision = cp.build_decision(req).with_mode(getattr(cp, "name", ""))
    if getattr(decision, "nf_action", "") == "RECONFIGURE_UPF":
        sim = getattr(host, "sim", None)
        if sim is not None:
            from sixg_sim.entities import UPF

            for ent_upf in sim.entities.values():
                if isinstance(ent_upf, UPF):
                    ent_upf.apply_reconfigure_upf()
            if getattr(sim, "_track_upf_super_metrics", False):
                sim.mark_upf_cp_reconfigure_completed()
    apply_fn = getattr(host, "_apply_control_plane_decision", None)
    if callable(apply_fn):
        apply_fn(orig, req, decision)

