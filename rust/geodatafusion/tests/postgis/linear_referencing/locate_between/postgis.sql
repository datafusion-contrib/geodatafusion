SELECT ST_AsEWKB(ST_LocateBetween(ST_GeomFromText($1), $2, $3)) AS result
