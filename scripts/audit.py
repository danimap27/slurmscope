#!/usr/bin/env python3
"""audit.py — auditoría ligera del propio código, sin dependencias.

Comprueba dos cosas que un linter pesado no hace falta para detectar en un proyecto de
este tamaño:

* que todos los ficheros compilan;
* que no hay importaciones sin usar (heurística sobre el árbol sintáctico).

Y calcula las métricas de la obra (ficheros, líneas, líneas de código sin comentarios,
funciones, clases y casos de prueba), que son las que se citan en la memoria técnica.

Uso:
    python3 scripts/audit.py                  # auditoría + métricas
    python3 scripts/audit.py --check          # solo auditoría (código de salida 1 si hay hallazgos)
    python3 scripts/audit.py --stats          # solo métricas
    python3 scripts/audit.py --json           # salida estructurada
"""

from __future__ import annotations

import argparse
import ast
import json
import py_compile
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

REPO = Path(__file__).resolve().parent.parent
PACKAGE = REPO / "slurmscope"
TESTS = REPO / "tests"
EXAMPLES = REPO / "examples"


def python_files(*directories: Path) -> List[Path]:
    found: List[Path] = []
    for directory in directories:
        if directory.is_dir():
            found.extend(sorted(directory.rglob("*.py")))
    return sorted(set(found))


def code_lines(path: Path) -> int:
    """Líneas con contenido que no son comentario."""
    total = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            total += 1
    return total


def unused_imports(path: Path) -> List[str]:
    """Nombres importados que no se usan en el fichero (heurística)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: Dict[str, int] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name.split(".")[0]] = node.lineno
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name != "*":
                    imported[alias.asname or alias.name] = node.lineno

    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            base = node
            while isinstance(base, ast.Attribute):
                base = base.value
            if isinstance(base, ast.Name):
                used.add(base.id)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            # los nombres usados en anotaciones escritas como cadena
            cleaned = node.value.replace("[", " ").replace("]", " ").replace(",", " ")
            used.update(word.strip("\"'(){}:.") for word in cleaned.split())

    return sorted(name for name in imported if name not in used and name != "annotations")


@dataclass
class Metrics:
    files: int = 0
    lines: int = 0
    code: int = 0
    functions: int = 0
    classes: int = 0
    tests: int = 0
    per_file: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, object]:
        return {
            "files": self.files,
            "lines": self.lines,
            "code": self.code,
            "functions": self.functions,
            "classes": self.classes,
            "tests": self.tests,
        }


def measure(paths: List[Path], count_tests: bool = False) -> Metrics:
    metrics = Metrics()
    for path in paths:
        text = path.read_text(encoding="utf-8")
        metrics.files += 1
        metrics.lines += len(text.splitlines())
        metrics.code += code_lines(path)
        tree = ast.parse(text)
        metrics.functions += sum(
            1 for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        )
        metrics.classes += sum(1 for node in ast.walk(tree) if isinstance(node, ast.ClassDef))
        if count_tests:
            metrics.tests += sum(
                1 for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
            )
        metrics.per_file[str(path.relative_to(REPO))] = {
            "lines": len(text.splitlines()),
            "code": code_lines(path),
        }
    return metrics


def run(check_only: bool, stats_only: bool, as_json: bool) -> int:
    sources = python_files(PACKAGE)
    tests = python_files(TESTS)
    examples = python_files(EXAMPLES)
    everything = sources + tests + examples

    findings: List[str] = []

    if not stats_only:
        for path in everything:
            try:
                py_compile.compile(str(path), doraise=True)
            except py_compile.PyCompileError as exc:
                findings.append(f"no compila: {path.relative_to(REPO)} ({exc.msg})")
        for path in everything:
            bad = unused_imports(path)
            if bad:
                findings.append(f"imports sin usar en {path.relative_to(REPO)}: {', '.join(bad)}")

    package = measure(sources)
    test_metrics = measure(tests, count_tests=True)
    example_metrics = measure(examples)

    if as_json:
        print(json.dumps(
            {
                "findings": findings,
                "package": package.as_dict(),
                "tests": test_metrics.as_dict(),
                "examples": example_metrics.as_dict(),
                "per_file": package.per_file,
            },
            indent=2, ensure_ascii=False, sort_keys=True,
        ))
        return 1 if findings else 0

    if not stats_only:
        if findings:
            print("Hallazgos:")
            for finding in findings:
                print(f"  · {finding}")
        else:
            print("Sin hallazgos: todo compila y no hay importaciones sin usar.")

    if not check_only:
        print()
        print(f"paquete   : {package.files} ficheros · {package.lines} líneas totales · "
              f"{package.code} líneas de código")
        print(f"pruebas   : {test_metrics.files} ficheros · {test_metrics.lines} líneas · "
              f"{test_metrics.tests} casos de prueba")
        print(f"ejemplos  : {example_metrics.files} ficheros · {example_metrics.lines} líneas")
        print(f"estructura: {package.functions} funciones/métodos · {package.classes} clases")

    return 1 if findings else 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Auditoría y métricas de SlurmScope")
    parser.add_argument("--check", action="store_true", help="solo auditoría")
    parser.add_argument("--stats", action="store_true", help="solo métricas")
    parser.add_argument("--json", action="store_true", help="salida JSON")
    args = parser.parse_args(argv)
    return run(check_only=args.check, stats_only=args.stats, as_json=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
