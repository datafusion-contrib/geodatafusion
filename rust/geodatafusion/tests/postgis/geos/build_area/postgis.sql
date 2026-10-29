SELECT ST_AsEWKB(ST_BuildArea(ST_GeomFromText($1))) AS geom
