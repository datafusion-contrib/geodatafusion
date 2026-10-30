from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import udaf, udf

from . import _rust
from ._rust import *
from ._rust import ___version

__version__: str = ___version()

if TYPE_CHECKING:
    from datafusion import SessionContext


def _register_exports(ctx: SessionContext, group: str) -> None:
    scalar_udfs, aggregate_udfs = _rust._registration_exports(group)

    for _, _, capsule in scalar_udfs:
        ctx.register_udf(udf(capsule))

    for _, _, capsule in aggregate_udfs:
        ctx.register_udaf(udaf(capsule))


def register_all_geo(ctx: SessionContext) -> None:
    """Register all `geo`-backed spatial UDFs on the given SessionContext."""
    _register_exports(ctx, "geo")


def register_all_geohash(ctx: SessionContext) -> None:
    """Register all GeoHash UDFs on the given SessionContext."""
    _register_exports(ctx, "geohash")


def register_all_native(ctx: SessionContext) -> None:
    """Register all native spatial UDFs and UDAFs on the given SessionContext."""
    _register_exports(ctx, "native")


def register_all(ctx: SessionContext) -> None:
    """Register all spatial UDFs and UDAFs defined in geodatafusion on the given SessionContext."""
    _register_exports(ctx, "all")
