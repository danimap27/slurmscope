"""SlurmScope — orquestación de barridos de experimentos en clústeres SLURM.

Ejecución de múltiples experimentos (job arrays), panel interactivo de resultados,
historial de ejecuciones y estimación de tiempos a partir del histórico.
"""

__version__ = "1.0.0"
__author__ = "Daniel Martín Pérez"
__license__ = "GPL-3.0-or-later"

from .config import SweepSpec, load_spec, ConfigError
from .matrix import Run, expand_runs, render_command, render_slurm_script
from .ledger import Ledger
from .backends import get_backend, RealSlurm, MockSlurm

__all__ = [
    "__version__",
    "SweepSpec",
    "load_spec",
    "ConfigError",
    "Run",
    "expand_runs",
    "render_command",
    "render_slurm_script",
    "Ledger",
    "get_backend",
    "RealSlurm",
    "MockSlurm",
]
