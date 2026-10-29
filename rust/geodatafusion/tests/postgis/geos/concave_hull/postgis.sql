SELECT ST_AsEWKB(ST_ConcaveHull(ST_GeomFromText($1), $2, $3)) AS geom
