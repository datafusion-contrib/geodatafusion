SELECT ST_AsEWKB(ST_MemUnion(g)) AS geometry
FROM (VALUES
    (ST_GeomFromText($1)),
    (ST_GeomFromText($2))
) AS t(g)
