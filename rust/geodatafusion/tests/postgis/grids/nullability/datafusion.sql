SELECT ST_AsEWKB(ST_Hexagon(arg2, 0, 0, ST_PointFromText(arg1, 4326))) AS hex_geom,
       ST_AsEWKB(ST_Square(arg2, 0, 0, ST_PointFromText(arg1, 4326))) AS square_geom,
       ST_Hexagon(arg2, 0, 0, ST_PointFromText(arg1, 4326)) IS NULL AS hex_is_null,
       ST_Square(arg2, 0, 0, ST_PointFromText(arg1, 4326)) IS NULL AS square_is_null
FROM oracle_input
