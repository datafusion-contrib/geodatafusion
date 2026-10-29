SELECT ST_AsEWKB(ST_TriangulatePolygon(ST_GeomFromText($1))) AS geom
