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


def test_st_makeline_register_all_typed_and_geometry_inputs():
    ctx = SessionContext()
    register_all(ctx)

    typed = Table(
        ctx.sql(
            """
            SELECT
                ST_MakeLine(ST_Point(0, 0, 4326), ST_PointZ(1, 2, 3, 4326)) AS line_geom,
                ST_AsText(ST_MakeLine(ST_Point(0, 0, 4326), ST_PointZ(1, 2, 3, 4326))) AS line_wkt,
                ST_MakeLine(NULL, ST_Point(0, 0, 4326)) AS null_geom
            """
        )
    )
    assert typed.schema.field("line_geom").metadata_str == {
        "ARROW:extension:metadata": '{"crs":"EPSG:4326","crs_type":"authority_code"}',
        "ARROW:extension:name": "geoarrow.linestring",
    }
    assert typed.schema.field("null_geom").metadata_str == {
        "ARROW:extension:metadata": '{"crs":"EPSG:4326","crs_type":"authority_code"}',
        "ARROW:extension:name": "geoarrow.geometry",
    }
    assert typed.column("line_wkt").to_pylist() == ["LINESTRING Z(0 0 0,1 2 3)"]
    assert typed.column("null_geom").to_pylist() == [None]

    wkt_rows = ctx.sql(
        """
        SELECT ST_AsText(ST_MakeLine(ST_GeomFromText(left_wkt), ST_GeomFromText(right_wkt))) AS line
        FROM (
            VALUES
                ('POINT(0 0)', 'LINESTRING(0 0,1 1,2 2)'),
                ('POINT Z(0 0 1)', 'POINT M(1 1 2)'),
                ('POINT EMPTY', 'POINT(1 2)'),
                ('POINT Z EMPTY', 'POINT EMPTY')
        ) AS inputs(left_wkt, right_wkt)
        """
    ).to_arrow_table()
    assert wkt_rows.to_pylist() == [
        {"line": "LINESTRING(0 0,1 1,2 2)"},
        {"line": "LINESTRING ZM(0 0 1 0,1 1 0 2)"},
        {"line": "LINESTRING(1 2)"},
        {"line": "LINESTRING Z EMPTY"},
    ]


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
