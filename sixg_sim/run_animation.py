"""Live terminal animation while a :class:`~sixg_sim.simulation.Simulation` runs."""

from __future__ import annotations

import math
import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sixg_sim.simulation import Simulation

# Packet marker moves UE → RAN → UPF → DN (4 hops).
_TOPOLOGY = [
    "   ╭────╮      ╭─────╮      ╭─────╮      ╭────╮",
    "   │ UE │ ───► │ RAN │ ───► │ UPF │ ───► │ DN │",
    "   ╰────╯      ╰─────╯      ╰─────╯      ╰────╯",
]
_HOP_COLS = (5, 18, 32, 45)  # column index for ● on each row (approx.)
_SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _animation_enabled() -> bool:
    if os.environ.get("SIXG_SIM_NO_ANIMATION", "").strip().lower() in ("1", "true", "yes"):
        return False
    if os.environ.get("NO_COLOR"):
        return False
    return hasattr(sys.stdout, "isatty") and bool(sys.stdout.isatty())


def _use_color() -> bool:
    return _animation_enabled() and not os.environ.get("SIXG_SIM_NO_COLOR")


def _c(code: str, text: str) -> str:
    if not _use_color():
        return text
    return f"\033[{code}m{text}\033[0m"


def _progress_bar(ratio: float, width: int = 30) -> str:
    ratio = max(0.0, min(1.0, ratio))
    filled = int(width * ratio)
    bar = "█" * filled + "░" * (width - filled)
    return _c("96", bar)


def _topology_frame(hop: int) -> list[str]:
    """Return three display lines with ● on hop 0..3."""
    hop = hop % 4
    lines = [list(row) for row in _TOPOLOGY]
    row = 1
    col = _HOP_COLS[hop]
    if col < len(lines[row]):
        lines[row][col] = "●"
    out: list[str] = []
    for chars in lines:
        line = "".join(chars)
        line = line.replace("●", _c("93;1", "●"))
        line = line.replace("─", _c("36", "─")).replace("►", _c("36", "►"))
        out.append(line)
    return out


class _RunAnimator:
    def __init__(self, sim: Simulation, until: float, *, title: str = "Running discrete-event simulation") -> None:
        self._sim = sim
        self._until = float(until)
        self._title = title
        self._finite_horizon = math.isfinite(self._until) and self._until > 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._active = False
        self._frame = 0

    def start(self) -> None:
        if not _animation_enabled():
            return
        self._stop.clear()
        sys.stdout.write("\033[?1049h\033[H\033[?25l")
        sys.stdout.flush()
        self._active = True
        self._thread = threading.Thread(target=self._loop, name="sixg-sim-anim", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._active:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        sys.stdout.write("\033[?1049h\033[?25h")
        sys.stdout.flush()
        self._active = False

    def _loop(self) -> None:
        while not self._stop.wait(0.08):
            self._draw()

    def _draw(self) -> None:
        sim = self._sim
        t = float(getattr(sim, "time", 0.0))
        q = len(getattr(sim, "event_queue", []))
        n_ev = int(getattr(sim, "events_processed", 0))
        hop = self._frame % 4
        self._frame += 1
        spin = _SPINNER[self._frame % len(_SPINNER)]

        topo = _topology_frame(hop)
        if self._finite_horizon:
            ratio = min(1.0, t / self._until)
            pct = 100.0 * ratio
            bar = _progress_bar(ratio)
            time_line = f"  {_c('1', spin)}  t = {t:8.3f} s / {self._until:.3f} s  ({pct:5.1f}%)  {bar}"
        else:
            bar = _c("96", "∞" + "░" * 28)
            time_line = f"  {_c('1', spin)}  t = {t:8.3f} s  (running)  {bar}"

        stats = (
            f"  events processed: {_c('95', str(n_ev))}   "
            f"scheduled remaining: {_c('95', str(q))}"
        )
        header = _c("1;96", f"  ◆ {self._title}")
        brand = _c("96", "  6GSimo 1.5") + _c("2", "  ·  packet traversing the core")

        block = "\n".join(["", brand, header, *topo, time_line, stats, ""])
        sys.stdout.write("\033[H\033[J")
        sys.stdout.write(block)
        sys.stdout.flush()


@contextmanager
def run_animation(sim: Simulation, until: float, *, title: str = "Running discrete-event simulation"):
    """Show a live topology animation while ``yield`` runs ``sim.run()`` (TTY only)."""
    anim = _RunAnimator(sim, until, title=title)
    anim.start()
    try:
        yield
    finally:
        anim.stop()
