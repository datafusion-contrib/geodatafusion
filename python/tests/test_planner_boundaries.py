"""Planner boundaries the Python registration entry point cannot change.

DataFusion 55 resolves a function call by looking up the scalar UDF registry
first, before the aggregate registry and before the window branch
(`datafusion-sql/src/expr/function.rs`: "User-defined function (UDF) should
have precedence"). A scalar and an aggregate therefore cannot share one name,
and no Python-side hook changes that: datafusion-python 55.0.0 exposes no
expression planner, and the DataFusion 55 `ExprPlanner` hooks run after name
resolution.

Aggregates are still usable in `OVER` clauses when nothing shadows them, which
is how a PostGIS-style window need can be met without new FFI surface.
"""

from __future__ import annotations

import pyarrow as pa
from datafusion import Accumulator, SessionContext, udaf

from geodatafusion import register_all


class _Constant(Accumulator):
    """Aggregate that always returns 42, so it is distinguishable from the
    scalar `ST_IsEmpty` it is registered against."""

    def __init__(self) -> None:
        self._value = pa.scalar(42, pa.int64())

    def state(self) -> list[pa.Scalar]:
        return [self._value]

    def update(self, values) -> None:
        pass

    def merge(self, states) -> None:
        pass

    def evaluate(self) -> pa.Scalar:
        return self._value


def test_scalar_udf_shadows_same_name_aggregate():
    ctx = SessionContext()
    register_all(ctx)
    ctx.register_udaf(
        udaf(
            _Constant,
            pa.bool_(),
            pa.int64(),
            [pa.int64()],
            "immutable",
            name="st_isempty",
        )
    )

    result = ctx.sql(
        """
        SELECT ST_IsEmpty(ST_GeomFromText(g)) AS v
        FROM (VALUES ('POINT(1 2)'), ('POINT(3 4)')) AS t(g)
        """
    ).to_arrow_table()

    # The scalar UDF wins, so this stays one row per input geometry instead of
    # collapsing into the aggregate's single row.
    assert result.num_rows == 2
    assert result.schema.field("v").type == pa.bool_()
    assert result.to_pylist() == [{"v": False}, {"v": False}]


def test_aggregate_udf_can_back_a_window_clause():
    ctx = SessionContext()
    register_all(ctx)

    result = ctx.sql(
        """
        SELECT ST_Extent(ST_GeomFromText(g)) OVER () AS extent
        FROM (VALUES ('POINT(1 2)'), ('POINT(3 4)')) AS t(g)
        """
    ).to_arrow_table()

    assert result.num_rows == 2
    assert result.to_pylist() == [
        {"extent": {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0}},
        {"extent": {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0}},
    ]
