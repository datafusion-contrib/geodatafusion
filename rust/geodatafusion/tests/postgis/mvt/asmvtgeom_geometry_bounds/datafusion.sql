SELECT ST_AsEWKB(
         ST_AsMVTGeom(
           ST_GeomFromText(arg1),
           ST_GeomFromText(arg2),
           arg3,
           arg4,
           arg5
         )
       ) AS geom
FROM oracle_input
