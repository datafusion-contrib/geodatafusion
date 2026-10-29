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


def test_st_linefrommultipoint_register_all_dimensions_and_nulls():
    ctx = SessionContext()
    register_all(ctx)

    nulls = Table(
        ctx.sql(
            """
            SELECT
                ST_LineFromMultiPoint(NULL) AS literal,
                ST_LineFromMultiPoint(geometry) AS column_value
            FROM (VALUES (NULL), (NULL)) AS inputs(geometry)
            """
        )
    )
    for name in ("literal", "column_value"):
        field = nulls.schema.field(name)
        assert field.metadata_str["ARROW:extension:name"] == "geoarrow.geometry"
        assert nulls.column(name).to_pylist() == [None, None]

    dimensions = ctx.sql(
        """
        SELECT ST_AsText(ST_LineFromMultiPoint(ST_GeomFromText(wkt))) AS line
        FROM (
            VALUES
                ('MULTIPOINT((1 2),(3 4))'),
                ('MULTIPOINT Z((1 2 3),(4 5 6))'),
                ('MULTIPOINT M((1 2 3),(4 5 6))'),
                ('MULTIPOINT ZM((1 2 3 4),(5 6 7 8))')
        ) AS inputs(wkt)
        """
    ).to_arrow_table()
    assert dimensions.to_pylist() == [
        {"line": "LINESTRING(1 2,3 4)"},
        {"line": "LINESTRING Z(1 2 3,4 5 6)"},
        {"line": "LINESTRING M(1 2 3,4 5 6)"},
        {"line": "LINESTRING ZM(1 2 3 4,5 6 7 8)"},
    ]
