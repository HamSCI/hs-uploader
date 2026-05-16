"""hs-uploader: library for shipping HamSCI sigmond observations to HF
reporting destinations.

See README.md / the design plan for architecture.  Public API:

* ``Record``, ``RecordBatch``, ``Outcome`` — data types.
* ``BatchPolicy``, ``RetryPolicy`` — knobs.
* ``Pipeline`` — one source bound to one transport.
* ``Uploader`` — orchestrator over N pipelines.
* ``StationIdentity`` — station-level config.
* sources/transports/watermark — concrete implementations in
  subpackages.
"""

from .config import StationIdentity
from .core import (
    BatchPolicy,
    Outcome,
    Pipeline,
    Record,
    RecordBatch,
    RetryPolicy,
    Uploader,
)

__version__ = "0.1.0"

__all__ = [
    "BatchPolicy",
    "Outcome",
    "Pipeline",
    "Record",
    "RecordBatch",
    "RetryPolicy",
    "StationIdentity",
    "Uploader",
    "__version__",
]
