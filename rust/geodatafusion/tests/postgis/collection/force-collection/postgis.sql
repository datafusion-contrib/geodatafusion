SELECT ST_AsEWKB(ST_ForceCollection(ST_GeomFromText($1))) AS geom
