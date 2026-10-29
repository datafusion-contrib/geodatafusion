SELECT ST_AsEWKB(ST_MakeLine(g ORDER BY id)) AS line
FROM (VALUES
    (2, ST_GeomFromText($1)),
    (1, ST_GeomFromText($2))
) AS t(id, g)
