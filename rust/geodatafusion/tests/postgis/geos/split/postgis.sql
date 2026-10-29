SELECT ST_AsEWKB(ST_Split(ST_GeomFromText($1), ST_GeomFromText($2))) AS geom
