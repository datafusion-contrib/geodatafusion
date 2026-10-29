SELECT ST_LineLocatePoint(ST_GeomFromText($1), ST_GeomFromText($2)) AS fraction, ST_InterpolatePoint(ST_GeomFromText($1), ST_GeomFromText($2)) AS measure
