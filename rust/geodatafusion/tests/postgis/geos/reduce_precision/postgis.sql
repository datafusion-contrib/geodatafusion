SELECT ST_AsEWKB(ST_ReducePrecision(ST_GeomFromText($1), $2)) AS geom
