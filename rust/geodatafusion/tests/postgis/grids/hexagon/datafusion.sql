SELECT ST_AsEWKB(ST_Hexagon($1, $2, $3, ST_PointFromText($4, 4326))) AS geom
