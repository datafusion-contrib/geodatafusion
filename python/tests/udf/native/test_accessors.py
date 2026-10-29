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


def test_st_numgeometries_register_all_members_nested_and_nulls():
    ctx = SessionContext()
    register_all(ctx)

    nulls = ctx.sql(
        """
        SELECT
            ST_NumGeometries(NULL) AS literal,
            ST_NumGeometries(geometry) AS column_value
        FROM (VALUES (NULL), (NULL)) AS inputs(geometry)
        """
    ).to_arrow_table()
    assert nulls.to_pylist() == [
        {"literal": None, "column_value": None},
        {"literal": None, "column_value": None},
    ]

    counts = ctx.sql(
        """
        SELECT ST_NumGeometries(ST_GeomFromText(wkt)) AS members
        FROM (
            VALUES
                ('POINT EMPTY'),
                ('POINT(1 2)'),
                ('GEOMETRYCOLLECTION(POINT EMPTY)'),
                ('GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 2))'),
                ('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)))'),
                ('GEOMETRYCOLLECTION(MULTIPOINT((1 2),(3 4)))')
        ) AS geometries(wkt)
        """
    ).to_arrow_table()
    assert counts.to_pylist() == [
        {"members": 0},
        {"members": 1},
        {"members": 0},
        {"members": 2},
        {"members": 1},
        {"members": 1},
    ]

    mixed_empty = ctx.sql(
        """
        SELECT ST_NumGeometries(
            ST_GeomFromWKB(
                X'0104000000020000000101000000000000000000f87f000000000000f87f010100000000000000000000000000000000000000'
            )
        ) AS members
        """
    ).to_arrow_table()
    assert mixed_empty.to_pylist() == [{"members": 2}]


def test_st_nrings_register_all_rings_and_nulls():
    ctx = SessionContext()
    register_all(ctx)

    nulls = ctx.sql(
        """
        SELECT
            ST_NRings(NULL) AS literal,
            ST_NRings(geometry) AS column_value
        FROM (VALUES (NULL), (NULL)) AS inputs(geometry)
        """
    ).to_arrow_table()
    assert nulls.to_pylist() == [
        {"literal": None, "column_value": None},
        {"literal": None, "column_value": None},
    ]

    rings = ctx.sql(
        """
        SELECT ST_NRings(ST_GeomFromText(wkt)) AS rings
        FROM (
            VALUES
                ('POLYGON((0 0,0 5,5 5,5 0,0 0))'),
                ('POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 2,2 2,1 1),(7 7,7 8,8 8,7 7))'),
                ('POLYGON EMPTY'),
                ('GEOMETRYCOLLECTION(POLYGON((0 0,0 5,5 5,0 0)),MULTIPOLYGON(((10 10,10 15,15 15,10 10))))'),
                ('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON EMPTY))'),
                ('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((0 0,0 5,5 5,0 0))))'),
                ('POINT(1 2)')
        ) AS geometries(wkt)
        """
    ).to_arrow_table()
    assert rings.to_pylist() == [
        {"rings": 1},
        {"rings": 3},
        {"rings": 0},
        {"rings": 2},
        {"rings": 0},
        {"rings": 1},
        {"rings": 0},
    ]
