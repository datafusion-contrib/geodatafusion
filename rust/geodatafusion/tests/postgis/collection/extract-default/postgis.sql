SELECT ST_AsEWKB(ST_CollectionExtract(ST_GeomFromText($1))) AS geom
