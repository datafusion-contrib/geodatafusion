from typing import Literal

from . import geo as geo, geohash as geohash, native as native

__all__ = ["geo", "geohash", "native"]

def ___version() -> str: ...

def _registration_exports(
    group: str,
    *,
    coord_type: Literal["interleaved", "separated"] | None = None,
) -> tuple[list[tuple[str, list[str], object]], list[tuple[str, list[str], object]]]: ...
