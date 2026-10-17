"""Panel interactivo (menú) para navegar por el estado del barrido.

Secciones: resumen, cola del clúster, ejecuciones, resultados, estimación, envíos
y fallos. Usa ``curses`` de la biblioteca estándar; si no está disponible o la
salida no es una terminal, cae a un menú de texto equivalente.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple

from .report import (
    Dashboard,
    render_failures,
    render_overview,
    render_queue,
    render_results,
    render_runs,
    render_submissions,
)

Section = Tuple[str, str, Callable[[Dashboard], str]]


def build_sections(dash: Dashboard) -> List[Section]:
    """Secciones del menú, en orden, con su renderizador."""
    return [
        ("1", "Resumen", render_overview),
        ("2", "Cola del clúster", lambda d: render_queue(d)),
        ("3", "Ejecuciones", lambda d: render_runs(d, limit=200)),
        ("4", "Resultados", lambda d: render_results(d)),
        ("5", "Estimación de tiempos", _render_estimate),
        ("6", "Envíos", render_submissions),
        ("7", "Fallos", render_failures),
    ]


def _render_estimate(dash: Dashboard) -> str:
    from .report import render_estimate_table

    return render_estimate_table(dash.estimate)


def render_section(dash: Dashboard, section_key: str) -> str:
    for key, _label, renderer in build_sections(dash):
        if key == section_key:
            return renderer(dash)
    return render_overview(dash)


@dataclass
class MenuAction:
    key: str
    label: str
    callback: Callable[[], Optional[str]]


class PlainMenu:
    """Menú de texto (sin curses): imprime y espera una tecla."""

    def __init__(self, refresh: Callable[[], Dashboard], actions: Sequence[MenuAction] = ()):
        self.refresh = refresh
        self.actions = list(actions)
        self.dash = refresh()
        self.section = "1"

    def _print(self, message: str = "") -> None:
        os.system("cls" if os.name == "nt" else "clear")
        print(self.dash.spec.project.center(78, "═"))
        print(render_section(self.dash, self.section))
        print()
        options = "  ".join(f"[{k}] {label}" for k, label, _ in build_sections(self.dash))
        print(options)
        extra = "  ".join(f"[{a.key}] {a.label}" for a in self.actions)
        if extra:
            print(extra)
        print("[r] Refrescar   [x] Informe   [q] Salir")
        if message:
            print(f"\n{message}")

    def run(self) -> int:
        while True:
            self._print()
            try:
                choice = input("\nopción> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            if choice in {"q", "quit", "salir"}:
                return 0
            if choice == "r":
                self.dash = self.refresh()
                continue
            if choice == "x":
                print("usa `slurmscope report` para exportar el informe")
                input("pulsa enter para continuar")
                continue
            if any(choice == key for key, _, _ in build_sections(self.dash)):
                self.section = choice
                continue
            changed = self._run_action(choice)
            if changed:
                self.dash = self.refresh()

    def _run_action(self, key: str) -> bool:
        for action in self.actions:
            if action.key == key:
                answer = input(f"{action.label} — ¿seguro? [s/N] ").strip().lower()
                if answer in {"s", "y", "si", "sí"}:
                    result = action.callback()
                    if result:
                        print(result)
                return True
        return False


class CursesMenu:
    """Panel interactivo con curses."""

    def __init__(self, refresh: Callable[[], Dashboard], actions: Sequence[MenuAction] = ()):
        self.refresh = refresh
        self.actions = list(actions)
        self.dash = refresh()
        self.section_index = 0

    def run(self) -> int:  # pragma: no cover - requiere terminal
        import curses

        return curses.wrapper(self._loop)

    def _loop(self, screen) -> int:  # pragma: no cover - requiere terminal
        import curses

        curses.curs_set(0)
        screen.nodelay(False)
        offset = 0
        message = ""

        while True:
            sections = build_sections(self.dash)
            key, label, renderer = sections[self.section_index]
            body = renderer(self.dash).splitlines()
            height, width = screen.getmaxyx()

            screen.erase()
            title = f" SlurmScope · {self.dash.spec.project} "
            screen.addstr(0, 0, title.ljust(width - 1), curses.A_REVERSE)

            tabs = "".join(
                f" {k}:{lbl} " for k, lbl, _ in sections
            )
            if len(tabs) > width - 1:
                tabs = tabs[: width - 2]
            attr = curses.A_BOLD
            screen.addstr(1, 0, tabs.ljust(width - 1), attr)

            visible = height - 4
            for line_no, text in enumerate(body[offset : offset + visible]):
                screen.addstr(2 + line_no, 0, text[: width - 1])

            footer = "[1-7] sección  ↑↓ scroll  [r] refrescar  [x] informe  [e] enviar  [c] cancelar  [q] salir"
            if message:
                footer = message + "   ·   " + footer
            screen.addstr(height - 1, 0, footer[: width - 1], curses.A_REVERSE)
            screen.refresh()

            ch = screen.getch()
            if ch in (ord("q"), ord("Q"), 27):
                return 0
            elif ch in (ord("1"), ord("2"), ord("3"), ord("4"), ord("5"), ord("6"), ord("7")):
                self.section_index = ch - ord("1")
                offset = 0
            elif ch in (curses.KEY_DOWN, ord("j")):
                offset = min(offset + 1, max(0, len(body) - visible))
            elif ch in (curses.KEY_UP, ord("k")):
                offset = max(offset - 1, 0)
            elif ch in (curses.KEY_NPAGE,):
                offset = min(offset + visible, max(0, len(body) - visible))
            elif ch in (curses.KEY_PPAGE,):
                offset = max(offset - visible, 0)
            elif ch in (ord("r"), ord("R")):
                self.dash = self.refresh()
                message = "estado refrescado"
            elif ch in (ord("e"), ord("E"), ord("c"), ord("C")):
                target = "e" if ch in (ord("e"), ord("E")) else "c"
                action = next((a for a in self.actions if a.key == target), None)
                if action:
                    result = action.callback()
                    message = (result or action.label)[: width - 40]
                    self.dash = self.refresh()


def run_menu(
    refresh: Callable[[], Dashboard],
    actions: Sequence[MenuAction] = (),
    force_plain: bool = False,
) -> int:
    """Lanza el panel interactivo, con curses si se puede."""
    use_curses = (
        not force_plain
        and sys.stdout.isatty()
        and os.environ.get("TERM", "") not in {"", "dumb"}
    )
    if use_curses:
        try:
            return CursesMenu(refresh, actions).run()
        except Exception:  # pragma: no cover - fallback defensivo
            pass
    return PlainMenu(refresh, actions).run()
