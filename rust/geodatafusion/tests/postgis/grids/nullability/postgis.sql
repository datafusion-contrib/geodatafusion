SELECT ST_AsEWKB(ST_Hexagon($2, 0, 0, ST_PointFromText($1, 4326))) AS hex_geom,
       ST_AsEWKB(ST_Square($2, 0, 0, ST_PointFromText($1, 4326))) AS square_geom,
       ST_Hexagon($2, 0, 0, ST_PointFromText($1, 4326)) IS NULL AS hex_is_null,
       ST_Square($2, 0, 0, ST_PointFromText($1, 4326)) IS NULL AS square_is_null
