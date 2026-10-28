SELECT dump.path::int[] AS path, ST_AsEWKB(dump.geom) AS geom
FROM ST_Dump(ST_GeomFromText($1)) AS dump
ORDER BY dump.path
