SELECT ST_AsEWKB(ST_SimplifyPolygonHull(ST_GeomFromText($1), $2, $3)) AS geom
