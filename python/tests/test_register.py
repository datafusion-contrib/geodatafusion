from __future__ import annotations

from datafusion import SessionContext
from geodatafusion import register_all


def test_register_all():
    # Ensure that register_all works without error
    ctx = SessionContext()
    register_all(ctx)


def test_st_dwithin_registered():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_DWithin(
                ST_GeomFromText('POINT(0 0)'),
                ST_GeomFromText('POINT(3 4)'),
                5
            ) AS within,
            ST_DWithin(
                ST_GeomFromText('POINT(0 0)'),
                ST_GeomFromText('POINT(3 4)'),
                4
            ) AS outside
        """
    ).to_arrow_table()

    assert result.column("within")[0].as_py() is True
    assert result.column("outside")[0].as_py() is False
