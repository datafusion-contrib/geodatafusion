SELECT ST_AsEWKB(
         ST_AsMVTGeom(
           ST_GeomFromText(arg1),
           ST_MakeBox2D(ST_Point(arg2, arg3), ST_Point(arg4, arg5)),
           arg6,
           arg7,
           arg8
         )
       ) AS geom
FROM oracle_input
