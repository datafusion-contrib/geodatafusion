from typing import Literal

class Force2D:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class Force3D:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class Force3DZ:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class Force3DM:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class Force4D:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...
