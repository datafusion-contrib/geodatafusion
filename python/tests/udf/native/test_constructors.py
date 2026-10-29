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
        "ARROW:extension:metadata": '{"crs":"EPSG:4326","crs_type":"authority_code"}',
        "ARROW:extension:name": "geoarrow.point",
    }


def test_st_makeenvelope_register_all_scalar_and_row_inputs():
    ctx = SessionContext()
    register_all(ctx)
    sql = """
    SELECT
        ST_MakeEnvelope(0, 1, 10, 6, 4326) AS scalar_geom,
        ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326) AS row_geom,
        ST_AsText(ST_MakeEnvelope(0, 1, 10, 6, 4326)) AS scalar_wkt,
        ST_AsText(
            ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)
        ) AS row_wkt
    FROM (
        VALUES (0.0, 1.0, 10.0, 6.0), (2.0, 3.0, 4.0, 5.0)
    ) AS envelopes(xmin, ymin, xmax, ymax)
    """
    table = Table(ctx.sql(sql))
    expected_metadata = {
        "ARROW:extension:metadata": '{"crs":"EPSG:4326","crs_type":"authority_code"}',
        "ARROW:extension:name": "geoarrow.polygon",
    }
    assert table.schema.field("scalar_geom").metadata_str == expected_metadata
    assert table.schema.field("row_geom").metadata_str == expected_metadata
    assert table.column("scalar_wkt").to_pylist() == [
        "POLYGON((0 1,0 6,10 6,10 1,0 1))",
        "POLYGON((0 1,0 6,10 6,10 1,0 1))",
    ]
    assert table.column("row_wkt").to_pylist() == [
        "POLYGON((0 1,0 6,10 6,10 1,0 1))",
        "POLYGON((2 3,2 5,4 5,4 3,2 3))",
    ]
