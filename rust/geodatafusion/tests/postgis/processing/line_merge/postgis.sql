SELECT ST_AsEWKB(ST_LineMerge(ST_GeomFromText($1), $2)) AS value
