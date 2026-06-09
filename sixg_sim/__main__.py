from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sixg_sim.banner import print_welcome
from sixg_sim.run_animation import animation_enabled, run_animation
from sixg_sim.control_plane import control_plane_summary_for_sim
from sixg_sim.entities import DataNetwork
from sixg_sim.modular import default_scenario_py_path, load_modular_scenario_py
from sixg_sim.simulation import (
    entity_queue_statistics,
    upf_experiment_summary,
    upf_super_metrics_summary,
    write_packet_lifecycle_csv,
)


def _scenario_adjacent_path(scenario_py: Path, output_file: Path) -> Path:
    """Place relative outputs next to the scenario module (absolute paths unchanged)."""
    out = Path(output_file)
    if out.is_absolute():
        return out
    return scenario_py.resolve().parent / out


def main() -> None:
    parser = argparse.ArgumentParser(description="6G Simo 1.5 — packet-level core network simulator (sixg-sim)")
    parser.add_argument(
        "--scenario",
        metavar="PATH",
        type=Path,
        default=None,
        help=(
            "Scenario ``.py`` (``build_simulation()`` or ``SCENARIO`` / ``MODULAR_SCENARIO``). "
            "When omitted, uses the package default (``examples/demo_simple.py``)."
        ),
    )
    parser.add_argument("--until", type=float, default=300.0, help="Simulation horizon (s)")
    parser.add_argument(
        "--packet-log",
        metavar="FILE",
        type=Path,
        default=None,
        help=(
            "Write packet lifecycle trace as CSV. If FILE is relative, it is placed in the "
            "same directory as --scenario. Default: ``packet_lifecycle.csv``."
        ),
    )
    args = parser.parse_args()
    # When animation runs it switches to the alternate screen and shows the banner there.
    if not animation_enabled():
        print_welcome()

    scenario_path = (args.scenario if args.scenario is not None else default_scenario_py_path()).resolve()
    if args.packet_log is None:
        args.packet_log = Path("packet_lifecycle.csv")

    sim = load_modular_scenario_py(scenario_path)

    with run_animation(sim, args.until):
        sim.run(until=args.until)
    dn_received = sum(
        getattr(ent, "packets_received", 0)
        for ent in sim.entities.values()
        if isinstance(ent, DataNetwork)
    )
    print(
        f"Simulation finished at t={sim.time:.6f}s, "
        f"events remaining={len(sim.event_queue)}, "
        f"DN packets={dn_received}"
    )

    cp_sum = control_plane_summary_for_sim(sim)
    if cp_sum:
        extra = ""
        if "avg_cp_waiting_s" in cp_sum:
            extra = (
                f", avg_cp_waiting_s={cp_sum['avg_cp_waiting_s']:.6f}"
                f", avg_cp_service_s={cp_sum['avg_cp_service_s']:.6f}"
            )
        dist = cp_sum.get("control_plane_ai_latency_dist", "")
        extra2 = f", dist={dist}" if dist else ""
        avg_samp = cp_sum.get("avg_sampled_service_time_s")
        extra3 = f", avg_sampled_service_s={avg_samp:.6f}" if avg_samp is not None else ""
        extra4 = ""
        if "llm_cost_total" in cp_sum:
            extra4 = (
                f", in_tok={cp_sum.get('input_tokens_total', 0)}"
                f", out_tok={cp_sum.get('output_tokens_total', 0)}"
                f", llm_cost={cp_sum.get('llm_cost_total', 0.0):.6f}"
            )
        print(
            "Control plane summary: "
            f"mode={cp_sum['control_plane_mode']}, "
            f"decisions={cp_sum.get('cp_decision_count', cp_sum.get('decision_count', 0))}, "
            f"ai_calls={cp_sum['ai_calls_total']}, "
            f"tokens={cp_sum['tokens_used_total']}, "
            f"total_cp_latency_s={cp_sum.get('cp_total_latency_time_s', cp_sum.get('total_cp_latency_s', 0.0)):.6f}, "
            f"avg_cp_latency_s={cp_sum.get('avg_cp_latency_s', 0.0):.6f}"
            f"{extra}{extra2}{extra3}{extra4}"
        )

    log_path = _scenario_adjacent_path(scenario_path, args.packet_log)
    if not sim.packet_tracing:
        print(
            "Warning: packet tracing is disabled for this scenario; packet lifecycle log will be empty.",
            file=sys.stderr,
        )
    write_packet_lifecycle_csv(log_path, sim)
    if sim.packet_lifecycle_capped:
        print(
            f"Warning: packet lifecycle log capped at {sim.packet_lifecycle_max_entries} rows.",
            file=sys.stderr,
        )
    print(f"Packet lifecycle CSV written to {log_path}")

    summary_path = _scenario_adjacent_path(scenario_path, Path("summary.json"))
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    upf_sum = upf_experiment_summary(sim)
    super_um = upf_super_metrics_summary(sim)
    payload = {
        "scenario_py": str(scenario_path),
        "until_s": float(args.until),
        "sim_time_s": float(sim.time),
        "control_plane": dict(cp_sum) if cp_sum else {},
        "nodes": entity_queue_statistics(sim),
        "avg_upf_latency_s": upf_sum["avg_upf_latency_s"],
        "p99_upf_latency_s": upf_sum["p99_upf_latency_s"],
        "upf_drop_count": upf_sum["upf_drop_count"],
        "upf_latency_sample_count": upf_sum["upf_latency_sample_count"],
    }
    payload.update(super_um)
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Summary JSON written to {summary_path}")
    if super_um:
        print(
            "S3_super episode metrics: "
            f"completed_episodes={super_um['upf_deg_episodes_completed']}, "
            f"wall_deg_s={super_um['upf_deg_wall_time_s']:.6f}, "
            f"backlog_integral_pkts_s={super_um['upf_deg_backlog_integral_packets_s']:.4f}, "
            f"mean_upf_lat_in_deg_episode_s={super_um['avg_upf_latency_in_cp_deg_episode_s']:.6g}, "
            f"n_ep_samples={super_um['upf_latency_in_cp_deg_episode_sample_count']}"
        )


if __name__ == "__main__":
    main()
