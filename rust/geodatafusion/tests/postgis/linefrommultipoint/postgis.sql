SELECT ST_AsEWKB(ST_LineFromMultiPoint(ST_GeomFromText($1))) AS geom
