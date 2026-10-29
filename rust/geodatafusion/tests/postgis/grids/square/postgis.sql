SELECT ST_AsEWKB(ST_Square($1, $2, $3, ST_PointFromText($4, 4326))) AS geom
