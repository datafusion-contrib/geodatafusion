from typing import Literal

__all__ = [
    "AsBinary",
    "GeomFromWKB",
    "AsText",
    "GeomFromText",
]

class AsBinary:
    def __init__(self) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class GeomFromWKB:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class AsText:
    def __init__(self) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class GeomFromText:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...
