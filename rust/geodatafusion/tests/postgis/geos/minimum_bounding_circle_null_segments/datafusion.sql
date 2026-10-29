SELECT ST_MinimumBoundingCircle(ST_GeomFromText(arg1), arg2) IS NULL AS is_null
FROM oracle_input
