SELECT ST_AsEWKB(ST_Normalize(ST_GeomFromText($1))) AS geom
