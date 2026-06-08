"""Terminal welcome banner for the 6G Simo CLI."""

from __future__ import annotations

import os
import sys

# Box-drawing glyphs: digit 6, letter G, then SIMO (not G+G).
_GLYPH_6 = [
    " ██████╗",
    "██╔════╝",
    "███████╗",
    "██╔══██║",
    "╚██████╔╝",
    " ╚═════╝",
]
_GLYPH_G = [
    " ██████╗",
    "██╔════╝",
    "██║  ███╗",
    "██║   ██║",
    "╚██████╔╝",
    " ╚═════╝",
]
_GLYPH_SIMO = [
    "███████╗██╗███╗   ███╗ ██████╗",
    "██╔════╝██║████╗ ████║██╔═══██╗",
    "███████╗██║██╔████╔██║██║   ██║",
    "╚════██║██║██║╚██╔╝██║██║   ██║",
    "███████║██║██║ ╚═╝ ██║╚██████╔╝",
    "╚══════╝╚═╝╚═╝     ╚═╝ ╚═════╝",
]

_BOX_TOP = "  ╔══════════════════════════════════════════════════════════════════╗"
_BOX_BOT = "  ╚══════════════════════════════════════════════════════════════════╝"
_BOX_PAD = "  ║"


def _merge_glyphs(*parts: list[str]) -> list[str]:
    sep = "  "
    return [sep.join(p[i] for p in parts) for i in range(len(parts[0]))]


def _banner_plain() -> str:
    title = _merge_glyphs(_GLYPH_6, _GLYPH_G, _GLYPH_SIMO)
    box_inner = 66

    def row(content: str) -> str:
        padded = content[:box_inner].ljust(box_inner)
        return f"{_BOX_PAD} {padded} ║"

    lines = [
        _BOX_TOP,
        row(""),
    ]
    for art in title:
        lines.append(row(art))
    lines.extend(
        [
            row(""),
            row("░▒▓  6GSimo  1.5  ▓▒░".center(box_inner)),
            row("packet-level discrete-event 6G core simulator".center(box_inner)),
            row(""),
            _BOX_BOT,
        ]
    )
    return "\n".join(lines)


_BANNER_PLAIN = _banner_plain()


def _ansi_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("SIXG_SIM_NO_COLOR"):
        return False
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _colorize(text: str) -> str:
    if not _ansi_enabled():
        return text
    cyan = "\033[96m"
    magenta = "\033[95m"
    yellow = "\033[93m"
    dim = "\033[2m"
    bold = "\033[1m"
    reset = "\033[0m"

    out: list[str] = []
    for line in text.splitlines():
        if "6GSimo" in line:
            out.append(f"{yellow}{bold}{line}{reset}")
        elif "packet-level" in line:
            out.append(f"{dim}{line}{reset}")
        elif "███████╗██╗███" in line or "██╔═══██╗" in line:
            out.append(f"{magenta}{bold}{line}{reset}")
        elif "███████╗" in line and "██╔══██" in line:
            # Middle row: digit 6 bar + SIMO start
            out.append(f"{cyan}{line}{reset}")
        elif "█" in line or "╔" in line or "╚" in line:
            out.append(f"{cyan}{line}{reset}")
        else:
            out.append(line)
    return "\n".join(out)


def print_welcome(*, force: bool = False) -> None:
    """Print the 6G Simo 1.5 welcome banner (TTY by default).

    Set ``SIXG_SIM_NO_BANNER=1`` to suppress. Pass ``force=True`` to print
    even when stdout is not a terminal.
    """
    if os.environ.get("SIXG_SIM_NO_BANNER", "").strip() in ("1", "true", "yes"):
        return
    if not force and hasattr(sys.stdout, "isatty") and not sys.stdout.isatty():
        return
    print(_colorize(_BANNER_PLAIN), flush=True)
    print(flush=True)
