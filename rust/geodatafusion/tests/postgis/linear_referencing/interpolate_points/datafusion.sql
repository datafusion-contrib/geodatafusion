SELECT ST_AsBinary(ST_LineInterpolatePoints(ST_GeomFromText($1), $2)) AS points
