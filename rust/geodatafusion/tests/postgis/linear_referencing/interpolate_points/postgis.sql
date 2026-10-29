SELECT ST_AsEWKB(ST_LineInterpolatePoints(ST_GeomFromText($1), $2)) AS points
