"""hs-uploader transport implementations.

Phase 1 ships the protocol base only; concrete transports
(WsprdaemonTarSftp, WsprnetMultipartPost, PskReporterTcp,
PswsDigitalRfSftp) land in subsequent phases.
"""

from .base import Transport

__all__ = ["Transport"]
