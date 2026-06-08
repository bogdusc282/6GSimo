"""6GSimo — simple demo scenario.

One UE on a classic 5GC-style star: UE → RAN → AMF / SMF / PCF → UPF → data network.

Illustrates:
- Discrete-event packet flow and NF queueing at each hop
- UE registration and PDU session (control-plane signalling packets)
- Baseline control plane (short deterministic decisions; mode 0 by default)
- Periodic uplink user traffic toward the DN

Run (default if you omit ``--scenario``)::

    python -m sixg_sim --until 60
    python -m sixg_sim --scenario sixg_sim/examples/demo_simple.py --until 60

Outputs ``summary.json`` and ``packet_lifecycle.csv`` next to this file.
"""

from __future__ import annotations

from sixg_sim.scenario import LinkProfile, ScenarioConfig, build_scenario
from sixg_sim.simulation import Simulation


def build_simulation() -> Simulation:
    return build_scenario(
        ScenarioConfig(
            num_ues=1,
            link=LinkProfile(),
            packet_tracing=True,
        )
    )
