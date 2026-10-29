SELECT ST_AsBinary(ST_LocateAlong(ST_GeomFromText($1), $2, $3)) AS points
