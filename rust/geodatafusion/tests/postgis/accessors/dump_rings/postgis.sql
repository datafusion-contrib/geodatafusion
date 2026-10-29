SELECT dump.path::int[] AS path, ST_AsEWKB(dump.geom) AS geom
FROM ST_DumpRings(ST_GeomFromText($1::text)) AS dump
ORDER BY dump.path
