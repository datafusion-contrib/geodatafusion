from __future__ import annotations

from datafusion import SessionContext
from geodatafusion import register_all


def test_register_all():
    # Ensure that register_all works without error
    ctx = SessionContext()
    register_all(ctx)
