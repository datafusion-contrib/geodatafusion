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


def registration_names(group: str) -> tuple[set[str], set[str], set[str], set[str]]:
    scalar_udfs, aggregate_udfs, window_udfs, table_functions = (
        _rust._registration_exports(group)
    )
    return (
        {name for name, _, _ in scalar_udfs},
        {name for name, _, _ in aggregate_udfs},
        {name for name, _, _ in window_udfs},
        {name for name, _ in table_functions},
    )


def test_register_all():
    # Ensure that register_all works without error
    ctx = SessionContext()
    register_all(ctx)


def test_register_all_matches_rust_registry():
    ctx = SessionContext()
    register_all(ctx)

    scalar_udfs, aggregate_udfs, window_udfs, _ = _rust._registration_exports("all")
    for name, aliases, _ in scalar_udfs:
        assert ctx.udf(name).name == name
        for alias in aliases:
            assert ctx.udf(alias).name == name

    for name, aliases, _ in aggregate_udfs:
        assert ctx.udaf(name).name == name
        for alias in aliases:
            assert ctx.udaf(alias).name == name

    for name, aliases, _ in window_udfs:
        assert ctx.udwf(name).name == name
        for alias in aliases:
            assert ctx.udwf(alias).name == name


def test_registration_groups_cover_rust_register():
    all_scalar, all_aggregate, all_window, all_table = registration_names("all")

    category_scalar: set[str] = set()
    category_aggregate: set[str] = set()
    category_window: set[str] = set()
    category_table: set[str] = set()
    for group, register in (
        ("geo", register_all_geo),
        ("geohash", register_all_geohash),
        ("native", register_all_native),
    ):
        scalar, aggregate, window, table = registration_names(group)
        category_scalar.update(scalar)
        category_aggregate.update(aggregate)
        category_window.update(window)
        category_table.update(table)

        ctx = SessionContext()
        register(ctx)
        for name in scalar:
            assert ctx.udf(name).name == name
        for name in aggregate:
            assert ctx.udaf(name).name == name
        for name in window:
            assert ctx.udwf(name).name == name
        # datafusion-python 55.0.0 exposes no table-function introspection, so
        # the names are compared as sets here and the real consumers run in
        # `test_register_all_registers_table_functions`.

    assert category_scalar == all_scalar
    assert category_aggregate == all_aggregate
    assert category_window == all_window
    assert category_table == all_table


def test_register_all_does_not_export_builtins():
    scalar_udfs, aggregate_udfs, _, _ = _rust._registration_exports("all")
    scalar_names = {name for name, _, _ in scalar_udfs}
    aggregate_names = {name for name, _, _ in aggregate_udfs}

    assert "abs" not in scalar_names
    assert "sum" not in aggregate_names
    assert "st_dump" in scalar_names
    assert "st_isempty" in scalar_names
    assert "st_extent" in aggregate_names
    # Async scalar UDFs capture the registering session catalog, and the
    # synchronous FFI capsule cannot carry that session. The Rust side filters
    # them generically through `ScalarUDF::as_async`; ST_EstimatedExtent is the
    # concrete instance that proves the filter fires.
    assert "st_estimatedextent" not in scalar_names


def test_register_all_covers_window_functions():
    """`register_all` exports every window UDF the Rust registry offers.

    The set is empty until the Rust crate grows window functions; once they
    land, this covers them generically instead of pinning names here.
    """
    _, _, window_udfs, _ = _rust._registration_exports("all")
    ctx = SessionContext()
    register_all(ctx)

    exported = {name for name, _, _ in window_udfs}
    for name, aliases, _ in window_udfs:
        assert ctx.udwf(name).name == name
        for alias in aliases:
            assert ctx.udwf(alias).name == name

    # Every exported window UDF must come from the Rust registry, not from a
    # hand-maintained list on the Python side.
    assert exported == registration_names("all")[2]


def test_register_all_registers_table_functions():
    """`register_all` carries the Rust registry's table functions into SQL."""
    _, _, _, table_functions = _rust._registration_exports("all")
    ctx = SessionContext()
    register_all(ctx)

    exported = {name for name, _ in table_functions}
    assert exported == registration_names("all")[3]
    assert "st_hexagongrid" in exported
    assert "st_squaregrid" in exported

    grid = ctx.sql(
        """
        SELECT count(*) AS cells
        FROM ST_HexagonGrid(1, NULL) AS grid
        """
    ).to_arrow_table()
    assert grid.to_pylist() == [{"cells": 0}]


def test_table_function_typed_literals_cross_the_ffi():
    """Typed literal arguments must still work once they lose their metadata.

    `datafusion-proto` serializes `Expr::Literal` without its field metadata and
    reconstructs it with `None`, so a GeoArrow-typed literal argument arrives as
    a bare Arrow shape. The grid functions only need that shape for a bounding
    box, so they recover it instead of rejecting the call. These cases crash the
    interpreter when the extension is built with a different compiler than the
    host, which is what `python/rust-toolchain.toml` pins.
    """
    ctx = SessionContext()
    register_all(ctx)

    def cells(sql: str) -> int:
        return ctx.sql(sql).to_arrow_table().to_pylist()[0]["cells"]

    # Bounds constructed by the extension itself.
    assert (
        cells(
            """
            SELECT count(*) AS cells
            FROM ST_HexagonGrid(1, ST_MakeEnvelope(0, 0, 10, 10))
            """
        )
        == 52
    )
    assert (
        cells(
            """
            SELECT count(*) AS cells
            FROM ST_SquareGrid(1, ST_MakeEnvelope(0, 0, 1.5, 1.5))
            """
        )
        == 4
    )

    # Bounds whose GeoArrow type is the union of every geometry kind, which is
    # what `ST_GeomFromText` returns.
    assert (
        cells(
            """
            SELECT count(*) AS cells
            FROM ST_SquareGrid(1, ST_GeomFromText('POLYGON((0 0,0 3,3 3,3 0,0 0))'))
            """
        )
        == 16
    )
    assert (
        cells(
            """
            SELECT count(*) AS cells
            FROM ST_HexagonGrid(2, ST_GeomFromText('LINESTRING(0 0, 5 5)'))
            """
        )
        == 6
    )

    # NULL bounds stay a valid empty grid.
    assert cells("SELECT count(*) AS cells FROM ST_HexagonGrid(1, NULL)") == 0


def test_table_function_registers_into_caller_catalog():
    """A table function sees the calling session's catalog through the FFI."""
    ctx = SessionContext()
    register_all(ctx)

    encoded = ctx.sql(
        """
        SELECT ST_AsFlatGeobuf(ST_PointFromText(wkt, 4326), id, label) AS fgb
        FROM (
            VALUES
                (1, 'point', 'POINT ZM(1 2 3 4)'),
                (2, NULL, 'POINT EMPTY'),
                (3, 'null-geom', NULL)
        ) AS input(id, label, wkt)
        """
    ).to_arrow_table()
    fgb = encoded.column("fgb")[0].as_py()
    assert len(fgb) > 0

    registered = ctx.sql(
        f"""
        SELECT count(*) AS rows
        FROM st_fromflatgeobuftotable('public', 'py_ffi_catalog', decode('{fgb.hex()}', 'hex'))
        """
    ).to_arrow_table()
    assert registered.to_pylist() == [{"rows": 3}]

    read_back = ctx.sql(
        """
        SELECT count(*) AS rows FROM py_ffi_catalog
        """
    ).to_arrow_table()
    assert read_back.to_pylist() == [{"rows": 3}]


def test_register_all_keeps_builtin_window_functions():
    ctx = SessionContext()
    builtin = set(ctx.udwfs())
    assert "row_number" in builtin

    register_all(ctx)

    assert set(ctx.udwfs()) >= builtin

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
