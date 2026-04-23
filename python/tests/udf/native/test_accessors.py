from __future__ import annotations

from arro3.core import Table
from datafusion import SessionContext
import geodatafusion
from geodatafusion import register_all


def test_st_is_closed_geoarrow():
    ctx = SessionContext()
    register_all(ctx)
    sql = "SELECT ST_IsClosed(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')) as geom"
    df = ctx.sql(sql)
    table = df.to_arrow_table()
    assert table.column("geom")[0].as_py() is True
