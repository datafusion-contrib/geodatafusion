SELECT ST_LargestEmptyCircle(ST_GeomFromText($1), $2, ST_GeomFromText($3)) IS NULL AS is_null
