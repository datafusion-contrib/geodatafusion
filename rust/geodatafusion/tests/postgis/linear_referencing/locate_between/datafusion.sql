SELECT ST_AsBinary(ST_LocateBetween(ST_GeomFromText($1), $2, $3)) AS result
