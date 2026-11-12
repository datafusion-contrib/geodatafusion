from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from datafusion import udaf, udf

from . import _rust
from ._rust import geo, geohash, native
from ._rust import ___version

__version__: str = ___version()

__all__ = [
    "__version__",
    "geo",
    "geohash",
    "native",
    "register_all",
    "register_all_geo",
    "register_all_geohash",
    "register_all_native",
]

if TYPE_CHECKING:
    from datafusion import SessionContext


def _register_exports(
    ctx: SessionContext,
    group: str,
    coord_type: Literal["interleaved", "separated"] | None,
) -> None:
    scalar_udfs, aggregate_udfs = _rust._registration_exports(
        group, coord_type=coord_type
    )

    for _, _, capsule in scalar_udfs:
        ctx.register_udf(udf(capsule))

    for _, _, capsule in aggregate_udfs:
        ctx.register_udaf(udaf(capsule))


def register_all_geo(
    ctx: SessionContext,
    *,
    coord_type: Literal["interleaved", "separated"] | None = None,
) -> None:
    """Register all `geo`-backed spatial UDFs on the given SessionContext.

    ``coord_type`` selects the layout for configurable geometry outputs.
    ``None`` uses the default separated layout; bounding boxes keep their fixed layout.
    """
    _register_exports(ctx, "geo", coord_type)


def register_all_geohash(
    ctx: SessionContext,
    *,
    coord_type: Literal["interleaved", "separated"] | None = None,
) -> None:
    """Register all GeoHash UDFs on the given SessionContext.

    ``coord_type`` selects the layout for configurable geometry outputs.
    ``None`` uses the default separated layout; bounding boxes keep their fixed layout.
    """
    _register_exports(ctx, "geohash", coord_type)


def register_all_native(
    ctx: SessionContext,
    *,
    coord_type: Literal["interleaved", "separated"] | None = None,
) -> None:
    """Register all native spatial UDFs and UDAFs on the given SessionContext.

    ``coord_type`` selects the layout for configurable geometry outputs.
    ``None`` uses the default separated layout; bounding boxes keep their fixed layout.
    """
    _register_exports(ctx, "native", coord_type)


def register_all(
    ctx: SessionContext,
    *,
    coord_type: Literal["interleaved", "separated"] | None = None,
) -> None:
    """Register all spatial UDFs and UDAFs defined in geodatafusion on the given SessionContext.

    ``coord_type`` selects the layout for configurable geometry outputs.
    ``None`` uses the default separated layout; bounding boxes keep their fixed layout.
    """
    _register_exports(ctx, "all", coord_type)
