#!/usr/bin/env python3
"""Genera docs/centros.md a partir de los perfiles (slurmscope/clusters_data.py).

Así el resumen de normas y lo que aplica el código no pueden divergir.
Uso: python3 scripts/gen_centros.py  (escribe docs/centros.md)
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from slurmscope.clusters_data import (  # noqa: E402
    PROFILES,
    RES_ACKNOWLEDGEMENT,
    RES_ACKNOWLEDGEMENT_NOTE,
)
from slurmscope.rules import slurm_time_seconds  # noqa: E402

HEADER = (REPO / "docs" / "centros_cabecera.md").read_text(encoding="utf-8")


def cell(value) -> str:
    text = "—" if value in (None, "", ()) else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def summary_table() -> str:
    rows = ["| Perfil | Máquina · centro | RES | Login | Obligatorio | Tiempo máx. | "
            "MaxArraySize | Sin internet |",
            "|---|---|---|---|---|---|---|---|"]
    for p in PROFILES.values():
        if p.name == "generic":
            continue
        required = [label for flag, label in ((p.requires_account, "account"),
                                              (p.requires_qos, "qos"),
                                              (p.requires_partition, "partición")) if flag]
        if p.qos_is_automatic:
            required.append("QoS automática")
        walls = [v.max_wall for v in {**p.qos_limits, **p.partition_limits}.values() if v.max_wall]
        wall = p.max_wall or (max(walls, key=lambda w: slurm_time_seconds(w) or 0)
                              if walls else None)
        rows.append("| `{}` | {} · {} | {} | {} | {} | {} | {} | {} |".format(
            p.name, p.title, p.centre, "sí" if p.res_node else "", cell(p.login),
            cell(", ".join(required)), cell(wall), cell(p.max_array_size),
            "sí" if p.internet_from_cluster is False else ""))
    return "\n".join(rows)


def detail(p) -> str:
    out = [f"### {p.title} · {p.centre}" + (" · nodo RES" if p.res_node else ""), ""]
    if p.logins:
        out.append(f"- **Login:** {', '.join(f'`{h}`' for h in p.logins)}")
    if p.access:
        out.append(f"- **Acceso:** {p.access}")
    part = p.partition or "la del clúster"
    if p.res_partition:
        part += f" · horas RES: `{p.res_partition}`"
    out.append(f"- **Partición:** {part}")
    if p.res_qos and not p.qos_is_automatic:
        out.append(f"- **QoS RES:** `{p.res_qos}`")
    if p.workdir_hint:
        out.append(f"- **Dónde trabajar:** {p.workdir_hint}")
    if p.internet_from_cluster is False:
        out.append("- **Internet:** sin conexiones salientes desde el clúster")
    if p.setup_hint:
        out.append(f"- **Software:** {p.setup_hint}")
    for line in p.directives + p.preamble:
        out.append(f"- **SlurmScope añade al script:** `{line}`")
    table = {**p.partition_limits, **p.qos_limits}
    if table:
        out += ["", "| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |",
                "|---|---|---|---|---|"]
        for name, v in table.items():
            out.append(f"| `{name}` | {cell(v.max_wall)} | {cell(v.max_jobs)} | "
                       f"{cell(v.max_submit)} | {cell(v.note)} |")
    if p.notes:
        out += ["", "Normas y recomendaciones:", ""]
        out += [f"- {n}" for n in p.notes]
    if p.acknowledgement:
        out += ["", "Agradecimiento:", "", f"> {p.acknowledgement}"]
    if p.docs:
        out += ["", "Fuentes: " + " · ".join(f"<{d}>" for d in p.docs)]
    if p.verified:
        out.append(f"Comprobado: {p.verified}.")
    return "\n".join(out)


def main() -> int:
    parts = [HEADER.rstrip(), "", "## Resumen", "", summary_table(), "",
             "Vacío = la documentación oficial no lo dice; `slurmscope probe` lo lee del clúster.",
             "", "## Agradecimiento común a la RES", "", f"> {RES_ACKNOWLEDGEMENT}", "",
             RES_ACKNOWLEDGEMENT_NOTE, "", "## Centros", ""]
    for p in PROFILES.values():
        if p.name != "generic":
            parts += [detail(p), ""]
    target = REPO / "docs" / "centros.md"
    target.write_text("\n".join(parts), encoding="utf-8")
    print(f"escrito {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
