SELECT ST_AsEWKB(ST_ForceSFS(ST_GeomFromText($1), $2)) AS geom
