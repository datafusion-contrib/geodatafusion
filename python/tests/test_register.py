from __future__ import annotations

import ast
from pathlib import Path

import pyarrow as pa
from datafusion import SessionContext, udaf, udf
from geodatafusion import (
    _rust,
    geo,
    geohash,
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


def test_line_merge_register_all():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText("
        "'MULTILINESTRING((0 0,1 1),(1 1,2 2))'))) AS line"
    ).to_arrow_table()
    assert result.to_pylist() == [{"line": "LINESTRING(0 0,1 1,2 2)"}]


def test_envelope_and_expand_register_all():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,1 3)'))) AS envelope,
            ST_AsText(ST_Expand(ST_GeomFromText('POINT(1 2)'), 1)) AS expanded
        """
    ).to_arrow_table()
    assert result.to_pylist() == [
        {
            "envelope": "POLYGON((0 0,0 3,1 3,1 0,0 0))",
            "expanded": "POLYGON((0 1,0 3,2 3,2 1,0 1))",
        }
    ]


def test_line_merge_directed():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText("
        "'MULTILINESTRING((0 0,1 1),(2 2,1 1))'), TRUE)) AS line"
    ).to_arrow_table()
    assert result.to_pylist() == [{"line": "MULTILINESTRING((0 0,1 1),(2 2,1 1))"}]


def test_line_merge_directed_with_multiple_rows():
    ctx = SessionContext()
    register_all(ctx)
    wkt = "MULTILINESTRING((0 0,1 1),(2 2,1 1))"
    ctx.register_record_batches("lines", [[pa.record_batch({"wkt": [wkt, wkt]})]])

    result = ctx.sql(
        "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText(wkt), FALSE)) AS line FROM lines"
    ).to_arrow_table()
    assert result.to_pylist() == [{"line": "LINESTRING(0 0,1 1,2 2)"}] * 2


def test_pyi_stubs_match_exported_classes():
    pkg_dir = Path(__file__).resolve().parent.parent / "python" / "geodatafusion"
    module_stubs = [
        (native, sorted((pkg_dir / "native").glob("_*.pyi"))),
        (geo, sorted((pkg_dir / "geo").glob("_*.pyi"))),
        (geohash, [pkg_dir / "geohash.pyi"]),
    ]

    for mod, stub_files in module_stubs:
        exported = {name for name in dir(mod) if not name.startswith("_")}
        stub_classes: dict[str, tuple[bool, set[str]]] = {}
        for stub_file in stub_files:
            tree = ast.parse(stub_file.read_text())
            for node in tree.body:
                if isinstance(node, ast.ClassDef):
                    methods: set[str] = set()
                    has_coord_type = False
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            methods.add(item.name)
                            if item.name == "__init__":
                                kwonly = {arg.arg for arg in item.args.kwonlyargs}
                                has_coord_type = "coord_type" in kwonly
                    stub_classes[node.name] = (has_coord_type, methods)

        assert exported == set(stub_classes), f"Mismatch in {mod.__name__}"

        for cls_name, (has_coord_type, methods) in stub_classes.items():
            cls = getattr(mod, cls_name)
            instance = cls(coord_type="interleaved") if has_coord_type else cls()
            if "__datafusion_scalar_udf__" in methods:
                assert hasattr(instance, "__datafusion_scalar_udf__")
                udf(instance)
            if "__datafusion_aggregate_udf__" in methods:
                assert hasattr(instance, "__datafusion_aggregate_udf__")
                udaf(instance)


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


def test_st_dimension_register_all():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT ST_Dimension(ST_GeomFromText(wkt)) AS dimension
        FROM (
            VALUES
                ('POINT EMPTY'),
                ('LINESTRING EMPTY'),
                ('POLYGON EMPTY'),
                ('MULTIPOINT EMPTY'),
                ('MULTILINESTRING EMPTY'),
                ('MULTIPOLYGON EMPTY'),
                ('GEOMETRYCOLLECTION EMPTY'),
                ('GEOMETRYCOLLECTION(POLYGON EMPTY)'),
                ('GEOMETRYCOLLECTION(LINESTRING EMPTY,POINT EMPTY)'),
                (NULL)
        ) AS inputs(wkt)
        """
    ).to_arrow_table()

    assert result.schema.field("dimension").type == pa.int32()
    assert result.to_pylist() == [
        {"dimension": 0},
        {"dimension": 1},
        {"dimension": 2},
        {"dimension": 0},
        {"dimension": 1},
        {"dimension": 2},
        {"dimension": 0},
        {"dimension": 2},
        {"dimension": 1},
        {"dimension": None},
    ]

    result = ctx.sql(
        """
        SELECT
            ST_Dimension(NULL) AS scalar_null,
            column_null
        FROM (
            SELECT ST_Dimension(value) AS column_null
            FROM (VALUES (NULL), (NULL)) AS inputs(value)
        )
        """
    ).to_arrow_table()
    assert result.schema.field("scalar_null").type == pa.int32()
    assert result.schema.field("column_null").type == pa.int32()
    assert result.to_pylist() == [
        {"scalar_null": None, "column_null": None},
        {"scalar_null": None, "column_null": None},
    ]


def test_st_point_n_register_all():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_PointN(ST_GeomFromText(wkt), n) AS point,
            ST_AsText(ST_PointN(ST_GeomFromText(wkt), n)) AS indexed,
            ST_AsText(ST_PointN(ST_GeomFromText(wkt), 1)) AS broadcast
        FROM (
            VALUES
                ('LINESTRING ZM (0 0 1 2,1 1 3 4,2 2 5 6)', 1),
                ('LINESTRING ZM (0 0 1 2,1 1 3 4,2 2 5 6)', -1),
                ('LINESTRING ZM (0 0 1 2,1 1 3 4,2 2 5 6)', 0),
                ('MULTILINESTRING((0 0,1 1))', 1),
                ('LINESTRING ZM EMPTY', 1),
                (NULL, 1)
        ) AS inputs(wkt, n)
        """
    ).to_arrow_table()

    point_field = result.schema.field("point")
    assert point_field.metadata.get(b"ARROW:extension:name") == b"geoarrow.geometry"
    rows = result.to_pylist()
    assert rows[0]["point"] is not None
    assert rows[1]["point"] is not None
    assert rows[2]["point"] is None
    assert rows[3]["point"] is None
    assert rows[4]["point"] is None
    assert rows[5]["point"] is None
    assert [row["indexed"] for row in rows] == [
        "POINT ZM(0 0 1 2)",
        "POINT ZM(2 2 5 6)",
        None,
        None,
        None,
        None,
    ]
    assert [row["broadcast"] for row in rows] == [
        "POINT ZM(0 0 1 2)",
        "POINT ZM(0 0 1 2)",
        "POINT ZM(0 0 1 2)",
        None,
        None,
        None,
    ]

    result = ctx.sql(
        """
        SELECT
            ST_PointN(NULL, 1) AS null_geometry,
            ST_PointN(
                ST_GeomFromText('LINESTRING(0 0,1 1)'),
                NULL
            ) AS null_index
        """
    ).to_arrow_table()
    assert (
        result.schema.field("null_geometry").metadata.get(b"ARROW:extension:name")
        == b"geoarrow.geometry"
    )
    assert (
        result.schema.field("null_index").metadata.get(b"ARROW:extension:name")
        == b"geoarrow.geometry"
    )
    assert result.to_pylist() == [{"null_geometry": None, "null_index": None}]

    result = ctx.sql(
        """
        SELECT ST_PointN(value, n) AS point
        FROM (VALUES (NULL, 1), (NULL, -1)) AS inputs(value, n)
        """
    ).to_arrow_table()
    assert (
        result.schema.field("point").metadata.get(b"ARROW:extension:name")
        == b"geoarrow.geometry"
    )
    assert result.to_pylist() == [{"point": None}, {"point": None}]


def test_st_interior_ring_n_register_all():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_InteriorRingN(ST_GeomFromText(wkt), n) AS ring,
            ST_AsText(ST_InteriorRingN(ST_GeomFromText(wkt), n)) AS indexed,
            ST_AsText(ST_InteriorRingN(ST_GeomFromText(wkt), 1)) AS broadcast
        FROM (
            VALUES
                ('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 1),(8 8,9 8,9 9,8 8))', 1),
                ('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 1),(8 8,9 8,9 9,8 8))', 2),
                ('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 1))', NULL),
                ('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))', 1)
        ) AS inputs(wkt, n)
        """
    ).to_arrow_table()

    ring_field = result.schema.field("ring")
    assert ring_field.metadata.get(b"ARROW:extension:name") == b"geoarrow.geometry"
    rows = result.to_pylist()
    assert isinstance(rows[0]["ring"], list)
    assert isinstance(rows[1]["ring"], list)
    assert rows[2]["ring"] is None
    assert rows[3]["ring"] is None
    assert [row["indexed"] for row in rows] == [
        "LINESTRING(1 1,2 1,2 2,1 1)",
        "LINESTRING(8 8,9 8,9 9,8 8)",
        None,
        None,
    ]
    assert [row["broadcast"] for row in rows] == [
        "LINESTRING(1 1,2 1,2 2,1 1)",
        "LINESTRING(1 1,2 1,2 2,1 1)",
        "LINESTRING(1 1,2 1,2 2,1 1)",
        None,
    ]

    result = ctx.sql(
        """
        SELECT
            ST_InteriorRingN(NULL, 1) AS null_geometry,
            ST_InteriorRingN(
                ST_GeomFromText(
                    'POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 1))'
                ),
                NULL
            ) AS null_index
        """
    ).to_arrow_table()
    assert (
        result.schema.field("null_geometry").metadata.get(b"ARROW:extension:name")
        == b"geoarrow.geometry"
    )
    assert (
        result.schema.field("null_index").metadata.get(b"ARROW:extension:name")
        == b"geoarrow.geometry"
    )
    assert result.to_pylist() == [{"null_geometry": None, "null_index": None}]


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


def test_register_all_st_iscollection_scalar_and_array():
    ctx = SessionContext()
    register_all(ctx)

    scalar = ctx.sql(
        """
        SELECT
            ST_IsCollection(ST_GeomFromText('POINT(1 2)')) AS point,
            ST_IsCollection(ST_GeomFromText('MULTIPOINT(1 2)')) AS multipoint,
            ST_IsCollection(
                ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')
            ) AS empty_collection,
            ST_IsCollection(ST_GeomFromText(NULL)) AS null_value,
            ST_IsCollection(NULL) AS raw_null
        """
    ).to_arrow_table()
    assert scalar.schema.field("point").type == pa.bool_()
    assert scalar.to_pylist() == [
        {
            "point": False,
            "multipoint": True,
            "empty_collection": True,
            "null_value": None,
            "raw_null": None,
        }
    ]

    array = ctx.sql(
        """
        SELECT ST_IsCollection(ST_GeomFromText(g)) AS is_collection
        FROM (
            VALUES (0, 'POINT(1 2)'), (1, 'MULTIPOINT(1 2)'),
                   (2, 'LINESTRING(0 0,1 1)'),
                   (3, 'MULTILINESTRING((0 0,1 1))')
        ) AS t(id, g)
        ORDER BY id
        """
    ).to_arrow_table()
    assert array.schema.field("is_collection").type == pa.bool_()
    assert array.column("is_collection").to_pylist() == [False, True, False, True]

    null_column = ctx.sql(
        """
        SELECT ST_IsCollection(g) AS is_collection
        FROM (VALUES (NULL), (NULL)) AS t(g)
        """
    ).to_arrow_table()
    assert null_column.schema.field("is_collection").type == pa.bool_()
    assert null_column.column("is_collection").to_pylist() == [None, None]


def test_register_all_st_zmflag_scalar_and_array():
    ctx = SessionContext()
    register_all(ctx)

    scalar = ctx.sql(
        """
        SELECT
            ST_Zmflag(ST_GeomFromText('POINT(1 2)')) AS xy,
            ST_Zmflag(ST_GeomFromText('POINT M(1 2 4)')) AS xym,
            ST_Zmflag(ST_GeomFromText('POINT Z(1 2 3)')) AS xyz,
            ST_Zmflag(ST_GeomFromText('POINT ZM(1 2 3 4)')) AS xyzm,
            ST_Zmflag(ST_GeomFromText(NULL)) AS null_value,
            ST_Zmflag(NULL) AS raw_null
        """
    ).to_arrow_table()
    assert scalar.schema.field("xy").type == pa.int16()
    assert scalar.to_pylist() == [
        {
            "xy": 0,
            "xym": 1,
            "xyz": 2,
            "xyzm": 3,
            "null_value": None,
            "raw_null": None,
        }
    ]

    array = ctx.sql(
        """
        SELECT ST_Zmflag(ST_GeomFromText(g)) AS zm_flag
        FROM (
            VALUES (0, 'POINT(1 2)'), (1, 'POINT M(1 2 4)'),
                   (2, 'POINT Z(1 2 3)'), (3, 'POINT ZM(1 2 3 4)')
        ) AS t(id, g)
        ORDER BY id
        """
    ).to_arrow_table()
    assert array.schema.field("zm_flag").type == pa.int16()
    assert array.column("zm_flag").to_pylist() == [0, 1, 2, 3]

    null_column = ctx.sql(
        """
        SELECT ST_Zmflag(g) AS zm_flag
        FROM (VALUES (NULL), (NULL)) AS t(g)
        """
    ).to_arrow_table()
    assert null_column.schema.field("zm_flag").type == pa.int16()
    assert null_column.column("zm_flag").to_pylist() == [None, None]


def test_register_all_st_hasz_scalar_and_array():
    ctx = SessionContext()
    register_all(ctx)

    scalar = ctx.sql(
        """
        SELECT
            ST_HasZ(ST_GeomFromText('POINT(1 2)')) AS xy,
            ST_HasZ(ST_GeomFromText('POINT Z(1 2 3)')) AS xyz,
            ST_HasZ(ST_GeomFromText('POINT M(1 2 4)')) AS xym,
            ST_HasZ(ST_GeomFromText('POINT ZM(1 2 3 4)')) AS xyzm,
            ST_HasZ(ST_GeomFromText(NULL)) AS null_value,
            ST_HasZ(NULL) AS raw_null
        """
    ).to_arrow_table()
    assert scalar.schema.field("xy").type == pa.bool_()
    assert scalar.to_pylist() == [
        {
            "xy": False,
            "xyz": True,
            "xym": False,
            "xyzm": True,
            "null_value": None,
            "raw_null": None,
        }
    ]

    array = ctx.sql(
        """
        SELECT ST_HasZ(ST_GeomFromText(g)) AS has_z
        FROM (
            VALUES (0, 'POINT(1 2)'), (1, 'POINT Z(1 2 3)'),
                   (2, 'POINT M(1 2 4)'), (3, 'POINT ZM(1 2 3 4)')
        ) AS t(id, g)
        ORDER BY id
        """
    ).to_arrow_table()
    assert array.schema.field("has_z").type == pa.bool_()
    assert array.column("has_z").to_pylist() == [False, True, False, True]


def test_register_all_st_hasm_scalar_and_array():
    ctx = SessionContext()
    register_all(ctx)

    scalar = ctx.sql(
        """
        SELECT
            ST_HasM(ST_GeomFromText('POINT(1 2)')) AS xy,
            ST_HasM(ST_GeomFromText('POINT Z(1 2 3)')) AS xyz,
            ST_HasM(ST_GeomFromText('POINT M(1 2 4)')) AS xym,
            ST_HasM(ST_GeomFromText('POINT ZM(1 2 3 4)')) AS xyzm,
            ST_HasM(ST_GeomFromText(NULL)) AS null_value,
            ST_HasM(NULL) AS raw_null
        """
    ).to_arrow_table()
    assert scalar.schema.field("xy").type == pa.bool_()
    assert scalar.to_pylist() == [
        {
            "xy": False,
            "xyz": False,
            "xym": True,
            "xyzm": True,
            "null_value": None,
            "raw_null": None,
        }
    ]

    array = ctx.sql(
        """
        SELECT ST_HasM(ST_GeomFromText(g)) AS has_m
        FROM (
            VALUES (0, 'POINT(1 2)'), (1, 'POINT Z(1 2 3)'),
                   (2, 'POINT M(1 2 4)'), (3, 'POINT ZM(1 2 3 4)')
        ) AS t(id, g)
        ORDER BY id
        """
    ).to_arrow_table()
    assert array.schema.field("has_m").type == pa.bool_()
    assert array.column("has_m").to_pylist() == [False, False, True, True]


def test_register_all_st_force_dimensions():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT
            ST_AsText(ST_Force2D(ST_GeomFromText('POINT ZM(1 2 3 4)'))) AS force_2d,
            ST_AsText(ST_Force3D(ST_GeomFromText('POINT(1 2)'), 9.0)) AS force_3d,
            ST_AsText(ST_Force3DZ(ST_GeomFromText('POINT M(1 2 4)'), 9.0)) AS force_3dz,
            ST_AsText(ST_Force3DM(ST_GeomFromText('POINT Z(1 2 3)'), 11.0)) AS force_3dm,
            ST_AsText(ST_Force4D(ST_GeomFromText('POINT(1 2)'), 9.0, 11.0)) AS force_4d,
            ST_AsText(ST_Force3DM(ST_3DMakeBox(ST_MakePoint(0, 1, 2), ST_MakePoint(3, 4, 5)), 8.0)) AS box3d_3dm,
            ST_AsText(ST_Force4D(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)))'), 3.0, 4.0)) AS nested_gc_4d,
            ST_Force4D(NULL, 9.0, 11.0) AS null_geom
        """
    ).to_arrow_table()

    assert (
        result.schema.field("null_geom").metadata.get(b"ARROW:extension:name")
        == b"geoarrow.geometry"
    )
    assert result.to_pylist() == [
        {
            "force_2d": "POINT(1 2)",
            "force_3d": "POINT Z(1 2 9)",
            "force_3dz": "POINT Z(1 2 9)",
            "force_3dm": "POINT M(1 2 11)",
            "force_4d": "POINT ZM(1 2 9 11)",
            "box3d_3dm": "POLYHEDRALSURFACE M(((0 1 8,0 4 8,3 4 8,3 1 8,0 1 8)),((0 1 8,3 1 8,3 4 8,0 4 8,0 1 8)),((0 1 8,0 1 8,0 4 8,0 4 8,0 1 8)),((3 1 8,3 4 8,3 4 8,3 1 8,3 1 8)),((0 1 8,3 1 8,3 1 8,0 1 8,0 1 8)),((0 4 8,0 4 8,3 4 8,3 4 8,0 4 8)))",
            "nested_gc_4d": "GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM(POINT ZM(1 2 3 4)))",
            "null_geom": None,
        }
    ]
