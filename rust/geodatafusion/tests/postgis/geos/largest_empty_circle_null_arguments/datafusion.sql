SELECT ST_LargestEmptyCircle(ST_GeomFromText(arg1), arg2, ST_GeomFromText(arg3)) IS NULL AS is_null
FROM oracle_input
