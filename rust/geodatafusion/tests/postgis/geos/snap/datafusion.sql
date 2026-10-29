SELECT ST_Snap(ST_GeomFromText(arg1), ST_GeomFromText(arg2), arg3) AS geom
FROM oracle_input
