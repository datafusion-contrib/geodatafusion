from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import TableFunction, udaf, udf, udwf

from . import _rust
from ._rust import *
from ._rust import ___version

__version__: str = ___version()

if TYPE_CHECKING:
    from datafusion import SessionContext


def _register_exports(ctx: SessionContext, group: str) -> None:
    scalar_udfs, aggregate_udfs, window_udfs, table_functions = (
        _rust._registration_exports(group)
    )

    for _, _, capsule in scalar_udfs:
        ctx.register_udf(udf(capsule))

    for _, _, capsule in aggregate_udfs:
        ctx.register_udaf(udaf(capsule))

    for _, _, capsule in window_udfs:
        ctx.register_udwf(udwf(capsule))

    for name, export in table_functions:
        ctx.register_udtf(TableFunction(name, export, ctx))


def register_all_geo(ctx: SessionContext):
    _register_exports(ctx, "geo")


def register_all_geohash(ctx: SessionContext):
    _register_exports(ctx, "geohash")


def register_all_native(ctx: SessionContext):
    _register_exports(ctx, "native")


def register_all(ctx: SessionContext):
    _register_exports(ctx, "all")
