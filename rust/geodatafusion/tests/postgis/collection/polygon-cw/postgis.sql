SELECT ST_AsEWKB(ST_ForcePolygonCW(ST_GeomFromText($1))) AS geom
