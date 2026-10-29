SELECT
    ST_SnapToGrid(ST_GeomFromText(arg1), arg2) AS xy,
    ST_SnapToGrid(ST_GeomFromText(arg1), arg3, arg4) AS xy_sizes,
    ST_SnapToGrid(ST_GeomFromText(arg1), arg5, arg6, arg3, arg4) AS origin_xy,
    ST_SnapToGrid(ST_GeomFromText(arg1), ST_GeomFromText(arg7), arg3, arg4, arg2, arg2) AS point_origin
FROM oracle_input
