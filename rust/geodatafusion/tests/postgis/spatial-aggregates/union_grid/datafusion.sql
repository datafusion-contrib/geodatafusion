SELECT ST_AsEWKB(ST_Union(g, $3)) AS geometry
FROM (VALUES
    (2, ST_GeomFromText($1)),
    (1, ST_GeomFromText($2))
) AS t(id, g)
