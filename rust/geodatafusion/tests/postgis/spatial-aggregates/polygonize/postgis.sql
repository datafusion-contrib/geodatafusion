SELECT ST_AsEWKB(ST_Polygonize(g)) AS geometry
FROM (VALUES
    (ST_GeomFromText($1)),
    (ST_GeomFromText($2))
) AS t(g)
