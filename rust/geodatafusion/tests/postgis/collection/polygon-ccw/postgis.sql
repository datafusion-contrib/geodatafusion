SELECT ST_AsEWKB(ST_ForcePolygonCCW(ST_GeomFromText($1))) AS geom
