SELECT ST_AsEWKB(g) AS geom FROM ST_Subdivide(ST_GeomFromText($1), CAST($2 AS bigint)::integer) AS g
