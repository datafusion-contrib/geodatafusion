SELECT ST_AsEWKB(ST_Multi(ST_GeomFromText($1))) AS geom
