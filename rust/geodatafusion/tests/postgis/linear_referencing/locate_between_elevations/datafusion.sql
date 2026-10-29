SELECT ST_AsBinary(ST_LocateBetweenElevations(ST_GeomFromText($1), $2, $3)) AS result
