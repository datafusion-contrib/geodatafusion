from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext, udf
from geodatafusion import (
    _rust,
    native,
    register_all,
    register_all_geo,
    register_all_geohash,
    register_all_native,
)


def registration_names(group: str) -> tuple[set[str], set[str]]:
    scalar_udfs, aggregate_udfs = _rust._registration_exports(group)
    return (
        {name for name, _, _ in scalar_udfs},
        {name for name, _, _ in aggregate_udfs},
    )


def test_register_all():
    # Ensure that register_all works without error
    ctx = SessionContext()
    register_all(ctx)


def test_register_all_matches_rust_registry():
    ctx = SessionContext()
    register_all(ctx)

    scalar_udfs, aggregate_udfs = _rust._registration_exports("all")
    for name, aliases, _ in scalar_udfs:
        assert ctx.udf(name).name == name
        for alias in aliases:
            assert ctx.udf(alias).name == name

    for name, aliases, _ in aggregate_udfs:
        assert ctx.udaf(name).name == name
        for alias in aliases:
            assert ctx.udaf(alias).name == name


def test_registration_groups_cover_rust_register():
    all_scalar, all_aggregate = registration_names("all")

    category_scalar: set[str] = set()
    category_aggregate: set[str] = set()
    for group, register in (
        ("geo", register_all_geo),
        ("geohash", register_all_geohash),
        ("native", register_all_native),
    ):
        scalar, aggregate = registration_names(group)
        category_scalar.update(scalar)
        category_aggregate.update(aggregate)

        ctx = SessionContext()
        register(ctx)
        for name in scalar:
            assert ctx.udf(name).name == name
        for name in aggregate:
            assert ctx.udaf(name).name == name

    assert category_scalar == all_scalar
    assert category_aggregate == all_aggregate


def test_register_all_does_not_export_builtins():
    scalar_udfs, aggregate_udfs = _rust._registration_exports("all")
    scalar_names = {name for name, _, _ in scalar_udfs}
    aggregate_names = {name for name, _, _ in aggregate_udfs}

    assert "abs" not in scalar_names
    assert "sum" not in aggregate_names
    assert "st_dump" in scalar_names
    assert "st_isempty" in scalar_names
    assert "st_extent" in aggregate_names

    ctx = SessionContext()
    register_all(ctx)
    result = ctx.sql(
        "SELECT abs(-3) AS absolute, sum(x) AS total FROM (VALUES (1), (2)) AS t(x)"
    ).to_arrow_table()
    assert result.to_pylist() == [{"absolute": 3, "total": 3}]


def test_existing_coord_type_option_still_works():
    ctx = SessionContext()
    register_all(ctx)
    # Exercise the individual wrapper API outside register_all's default export.
    ctx.register_udf(udf(native.EndPoint(coord_type="interleaved")))

    result = ctx.sql(
        "SELECT ST_AsText(ST_EndPoint(ST_GeomFromText('LINESTRING(0 0, 1 1)'))) AS point"
    ).to_arrow_table()
    assert result.to_pylist() == [{"point": "POINT(1 1)"}]


def test_st_dump_list_and_unnest():
    ctx = SessionContext()
    register_all(ctx)

    dumped = ctx.sql(
        "SELECT ST_Dump(ST_GeomFromText('MULTIPOINT(0 0, 1 1)')) AS dumped"
    ).to_arrow_table()
    dumped_field = dumped.schema.field("dumped")
    assert pa.types.is_list(dumped_field.type)
    assert dumped_field.type.value_field.type.field("geom").metadata == {
        b"ARROW:extension:name": b"geoarrow.geometry"
    }
    assert dumped.to_pylist() == [
        {
            "dumped": [
                {"path": [1], "geom": {"x": 0.0, "y": 0.0}},
                {"path": [2], "geom": {"x": 1.0, "y": 1.0}},
            ]
        }
    ]

    unnested = ctx.sql(
        """
        SELECT ST_AsText(comp.geom) AS wkt, comp.path AS path
        FROM (
            SELECT unnest(
                ST_Dump(ST_GeomFromText('MULTIPOINT(0 0, 1 1)'))
            ) AS comp
        )
        """
    ).to_arrow_table()
    assert unnested.to_pylist() == [
        {"wkt": "POINT(0 0)", "path": [1]},
        {"wkt": "POINT(1 1)", "path": [2]},
    ]


def test_st_dump_null_and_empty():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_Dump(ST_GeomFromText(NULL)) AS null_dump,
            ST_Dump(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')) AS empty_dump
        """
    ).to_arrow_table()
    assert result.to_pylist() == [{"null_dump": None, "empty_dump": []}]


def test_st_exterior_ring_register_all():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_ExteriorRing(ST_GeomFromText(wkt)) AS ring,
            ST_AsText(ST_ExteriorRing(ST_GeomFromText(wkt))) AS exterior
        FROM (
            VALUES
                ('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,2 2,2 1,1 1))'),
                ('POLYGON EMPTY'),
                ('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))'),
                (NULL)
        ) AS inputs(wkt)
        """
    ).to_arrow_table()

    ring_field = result.schema.field("ring")
    assert ring_field.metadata.get(b"ARROW:extension:name") == b"geoarrow.geometry"
    assert result.to_pylist() == [
        {
            "ring": [
                {"x": 0.0, "y": 0.0},
                {"x": 4.0, "y": 0.0},
                {"x": 4.0, "y": 4.0},
                {"x": 0.0, "y": 4.0},
                {"x": 0.0, "y": 0.0},
            ],
            "exterior": "LINESTRING(0 0,4 0,4 4,0 4,0 0)",
        },
        {"ring": [], "exterior": "LINESTRING EMPTY"},
        {"ring": None, "exterior": None},
        {"ring": None, "exterior": None},
    ]


def test_st_exterior_ring_register_all_untyped_null():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql("SELECT ST_ExteriorRing(NULL) AS value").to_arrow_table()

    value_field = result.schema.field("value")
    assert value_field.metadata.get(b"ARROW:extension:name") == b"geoarrow.geometry"
    assert result.to_pylist() == [{"value": None}]

    result = ctx.sql(
        """
        SELECT ST_ExteriorRing(value) AS value
        FROM (VALUES (NULL), (NULL)) AS inputs(value)
        """
    ).to_arrow_table()

    value_field = result.schema.field("value")
    assert value_field.metadata.get(b"ARROW:extension:name") == b"geoarrow.geometry"
    assert result.to_pylist() == [{"value": None}, {"value": None}]


def test_st_isempty_and_extent():
    ctx = SessionContext()
    register_all(ctx)

    empty = ctx.sql(
        """
        SELECT
            ST_IsEmpty(ST_GeomFromText('POINT EMPTY')) AS empty,
            ST_IsEmpty(ST_GeomFromText(NULL)) AS null_empty,
            ST_IsEmpty(ST_GeomFromText('POINT(1 2)')) AS point
        """
    ).to_arrow_table()
    assert empty.schema.field("empty").type == pa.bool_()
    assert empty.to_pylist() == [{"empty": True, "null_empty": None, "point": False}]

    extent = ctx.sql(
        """
        SELECT ST_Extent(ST_GeomFromText(g)) AS extent
        FROM (VALUES ('POINT(1 2)'), ('POINT(3 4)')) AS t(g)
        """
    ).to_arrow_table()
    assert extent.to_pylist() == [
        {
            "extent": {
                "xmin": 1.0,
                "ymin": 2.0,
                "xmax": 3.0,
                "ymax": 4.0,
            }
        }
    ]


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
            ) AS outside,
            ST_DWithin(
                ST_GeomFromText('POINT EMPTY'),
                ST_GeomFromText('POINT(0 0)'),
                5
            ) AS empty,
            ST_DWithin(
                NULL,
                ST_GeomFromText('POINT(0 0)'),
                -1
            ) AS null_wins_invalid_radius
        """
    ).to_arrow_table()

    assert result.column("within")[0].as_py() is True
    assert result.column("outside")[0].as_py() is False
    assert result.column("empty")[0].as_py() is False
    assert result.column("null_wins_invalid_radius")[0].as_py() is None
