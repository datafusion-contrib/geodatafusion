from __future__ import annotations

from datafusion import SessionContext
from geoarrow.types.type_pyarrow import register_extension_types
from geodatafusion import register_all

register_extension_types()


def test_st_point_crs_geoarrow():
    # Ensure that register_all works without error
    ctx = SessionContext()
    register_all(ctx)
    sql = "SELECT ST_Point(0, 1, 4326) as geom"
    df = ctx.sql(sql)
    schema = df.schema()
    assert schema.field("geom").metadata == {
        b"ARROW:extension:metadata": b'{"crs":"4326","crs_type":"srid"}',
        b"ARROW:extension:name": b"geoarrow.point",
    }
