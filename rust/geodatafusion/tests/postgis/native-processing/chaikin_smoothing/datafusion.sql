SELECT ST_ChaikinSmoothing(ST_GeomFromText($1), $2, $3) AS geom
