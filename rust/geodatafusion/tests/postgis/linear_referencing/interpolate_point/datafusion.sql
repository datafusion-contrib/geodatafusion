SELECT ST_AsBinary(ST_LineInterpolatePoint(ST_GeomFromText($1), $2)) AS point, ST_AsBinary(ST_3DLineInterpolatePoint(ST_GeomFromText($1), $2)) AS point_3d
