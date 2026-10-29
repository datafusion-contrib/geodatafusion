SELECT ST_AsEWKB(ST_Union(g, CAST($3 AS double precision))) AS geometry
FROM (VALUES
    (2, ST_GeomFromText($1)),
    (1, ST_GeomFromText($2))
) AS t(id, g)
