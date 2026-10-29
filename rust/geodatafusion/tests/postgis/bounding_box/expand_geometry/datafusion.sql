SELECT ST_AsEWKB(ST_Expand(ST_GeomFromText($1), $2, $3, $4)) AS geom
