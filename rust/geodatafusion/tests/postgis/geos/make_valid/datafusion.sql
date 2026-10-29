SELECT ST_AsEWKB(ST_MakeValid(ST_GeomFromText($1), $2)) AS geom
