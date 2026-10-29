SELECT ST_AsEWKB(s.comp.geom) AS geom FROM (SELECT unnest(ST_Subdivide(ST_GeomFromText($1), $2)) AS comp) AS s
