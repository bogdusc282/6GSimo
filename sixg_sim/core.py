from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum, auto
from typing import Any


class TrafficType(Enum):
    NORMAL = auto()
    AI_TRAINING = auto()
    AI_INFERENCE = auto()


class PacketPlane(Enum):
    """User-plane data vs control-plane signalling; both use the same :class:`Packet` type."""

    USER = auto()
    CONTROL = auto()


class UpfQosClass(IntEnum):
    """UPF user-plane queue when :attr:`~sixg_sim.entities.UPF.dual_user_qos_queues` is enabled.

    ``PRIORITY`` shares the strict-priority ingress with synthetic ``UPF_BACKGROUND_WORK`` (served before best-effort).
    """

    BEST_EFFORT = 0
    PRIORITY = 1


def coerce_upf_qos_class(value: Any, default: UpfQosClass) -> UpfQosClass:
    """Parse ``upf_qos_class`` from session QoS dicts, SMF rules, or scenario."""
    if value is None:
        return default
    if isinstance(value, UpfQosClass):
        return value
    if isinstance(value, int):
        try:
            return UpfQosClass(value)
        except ValueError:
            return default
    if isinstance(value, str):
        v = value.strip().upper().replace("-", "_")
        if v in ("PRIORITY", "1", "HIGH"):
            return UpfQosClass.PRIORITY
        if v in ("BEST_EFFORT", "0", "BE", "LOW"):
            return UpfQosClass.BEST_EFFORT
    return default


@dataclass(order=True)
class Event:
    time: float
    priority: int
    seq: int
    target: Any = field(compare=False)
    event_type: str = field(compare=False)
    payload: Any = field(compare=False, default=None)


@dataclass
class Packet:
    """Unified PDU: user traffic or encapsulated control signalling.

    All messages traverse links as ``PACKET_ARRIVAL`` events carrying this object.
    """

    plane: PacketPlane
    src: str
    dst: str
    creation_time: float
    # User plane (``plane == PacketPlane.USER``)
    ue_id: str = ""
    session_id: int = 0
    qos_flow_id: int = 0
    size_bytes: int = 0
    traffic_type: TrafficType = TrafficType.NORMAL
    upf_qos_class: UpfQosClass = UpfQosClass.BEST_EFFORT
    # Control plane (``plane == PacketPlane.CONTROL``): NAS/SMF/AI message type + payload
    msg_type: str = ""
    control_payload: dict[str, Any] = field(default_factory=dict)
    # Filled by :class:`Simulation` on first trace/log; stable for lifecycle views
    trace_id: str = ""
    # Set when the packet enters the UPF ingress (mixed-traffic validation / delay accounting).
    upf_ingress_time: float | None = None
    # End of UPF node service (user-plane PDUs).
    upf_egress_time: float | None = None
    #: QoS label for logging (e.g. ``"URLLC"`` for S3 DES traffic).
    qos: str = ""

    @staticmethod
    def user_data(
        *,
        ue_id: str,
        session_id: int,
        qos_flow_id: int,
        src: str,
        dst: str,
        size_bytes: int,
        traffic_type: TrafficType,
        creation_time: float,
        trace_id: str = "",
        upf_qos_class: UpfQosClass = UpfQosClass.BEST_EFFORT,
        qos: str = "",
    ) -> Packet:
        return Packet(
            plane=PacketPlane.USER,
            src=src,
            dst=dst,
            creation_time=creation_time,
            ue_id=ue_id,
            session_id=session_id,
            qos_flow_id=qos_flow_id,
            size_bytes=size_bytes,
            traffic_type=traffic_type,
            upf_qos_class=upf_qos_class,
            trace_id=trace_id,
            qos=str(qos),
        )

    @staticmethod
    def control_signal(
        msg_type: str,
        *,
        src: str,
        dst: str,
        payload: dict[str, Any],
        creation_time: float,
        trace_id: str = "",
    ) -> Packet:
        return Packet(
            plane=PacketPlane.CONTROL,
            src=src,
            dst=dst,
            creation_time=creation_time,
            msg_type=msg_type,
            control_payload=dict(payload),
            trace_id=trace_id,
        )
