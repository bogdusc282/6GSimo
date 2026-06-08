from __future__ import annotations

from sixg_sim.scenario import LinkProfile, ScenarioConfig, build_scenario
from sixg_sim.simulation import Simulation


def build_simple_topology() -> Simulation:
    """Single UE star topology (RAN, AMF/SMF/PCF/UPF, DN)."""
    return build_scenario(
        ScenarioConfig(
            num_ues=1,
            link=LinkProfile(),
        )
    )
