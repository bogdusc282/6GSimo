"""Packet-level discrete-event 6GSimo 1.5 engine with classical 5G-evolved NFs.

The engine (`Simulation`) advances time by processing a priority queue of `Event`
targets. Network functions inherit `Entity` and exchange `Packet` (user or control
plane) over `Link` latency models.

Typical minimal scenario: UE registration and PDU session setup (AMF/SMF/PCF),
UPF rule installation, and periodic UE traffic.
"""

from sixg_sim.core import (
    Event,
    Packet,
    PacketPlane,
    TrafficType,
    UpfQosClass,
    coerce_upf_qos_class,
)
from sixg_sim.simulation import (
    Entity,
    Link,
    Simulation,
    entity_queue_statistics,
    group_packet_lifecycle,
    packet_arrival_span_records,
    packet_lifecycle_span_records,
    write_packet_lifecycle_csv,
)
from sixg_sim.scenario import (
    LinkProfile,
    ScenarioConfig,
    build_scenario,
    effective_queue_capacities,
    sample_ue_ran_link_latency,
)
from sixg_sim.topology import build_simple_topology
from sixg_sim.modular import (
    LinkSpec,
    ModularScenarioSpec,
    NetworkNodeKind,
    NetworkNodeSpec,
    SmfRulePlanFn,
    TrafficArrivalProcess,
    TrafficFlowSpec,
    TrafficNextDelayFn,
    build_modular_simulation,
    default_scenario_py_path,
    load_modular_scenario_py,
)

__all__ = [
    "PacketPlane",
    "Event",
    "Packet",
    "TrafficType",
    "UpfQosClass",
    "coerce_upf_qos_class",
    "Entity",
    "Link",
    "Simulation",
    "entity_queue_statistics",
    "group_packet_lifecycle",
    "packet_lifecycle_span_records",
    "packet_arrival_span_records",
    "write_packet_lifecycle_csv",
    "build_simple_topology",
    "LinkProfile",
    "effective_queue_capacities",
    "sample_ue_ran_link_latency",
    "ScenarioConfig",
    "build_scenario",
    "NetworkNodeSpec",
    "NetworkNodeKind",
    "LinkSpec",
    "TrafficFlowSpec",
    "TrafficArrivalProcess",
    "ModularScenarioSpec",
    "TrafficNextDelayFn",
    "SmfRulePlanFn",
    "build_modular_simulation",
    "default_scenario_py_path",
    "load_modular_scenario_py",
]
