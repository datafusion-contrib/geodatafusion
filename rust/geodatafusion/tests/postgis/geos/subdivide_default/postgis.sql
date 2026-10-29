SELECT ST_AsEWKB(g) AS geom FROM ST_Subdivide(ST_GeomFromText($1)) AS g
