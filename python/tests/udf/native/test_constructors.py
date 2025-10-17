from __future__ import annotations

from arro3.core import Table
from datafusion import SessionContext
from geodatafusion import register_all


def test_st_point_crs_geoarrow():
    # Ensure that register_all works without error
    ctx = SessionContext()
    register_all(ctx)
    sql = "SELECT ST_Point(0, 1, 4326) as geom"
    df = ctx.sql(sql)
    schema = Table(df).schema
    assert schema.field("geom").metadata_str == {
        "ARROW:extension:metadata": '{"crs":"4326","crs_type":"srid"}',
        "ARROW:extension:name": "geoarrow.point",
    }
