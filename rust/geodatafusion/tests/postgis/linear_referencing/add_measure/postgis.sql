SELECT ST_AsEWKB(ST_AddMeasure(ST_GeomFromText($1), $2, $3)) AS result
