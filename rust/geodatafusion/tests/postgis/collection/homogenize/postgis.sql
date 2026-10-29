SELECT ST_AsEWKB(ST_CollectionHomogenize(ST_GeomFromText($1))) AS geom
