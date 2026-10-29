SELECT ST_RemoveIrrelevantPointsForView(
    ST_GeomFromText(arg1),
    ST_MakeBox2D(ST_Point(arg2, arg3), ST_Point(arg4, arg5)),
    arg6
) AS geom
FROM oracle_input
