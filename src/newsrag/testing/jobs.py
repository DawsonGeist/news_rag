"""Extraction jobs with injected faults, for testing the writer loop's pool handling.
They live in the package so pool worker processes can import them by name."""

from __future__ import annotations

import os
import time

from newsrag.contracts.v1 import Document
from newsrag.extract.extractor import ExtractContext, ExtractFailure, extract

HANG_MARKER = b"<!--newsrag-test:hang-->"
CRASH_MARKER = b"<!--newsrag-test:crash-->"


def faulty_extract(body: bytes, ctx: ExtractContext) -> Document | ExtractFailure:
    if body.startswith(HANG_MARKER):
        time.sleep(3600)
    if body.startswith(CRASH_MARKER):
        os._exit(1)  # a worker dying mid-task, like a segfault in a C extension
    return extract(body, ctx)
