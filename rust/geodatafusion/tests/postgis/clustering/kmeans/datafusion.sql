SELECT CAST(id AS INT) AS id, ST_ClusterKMeans(geom, CAST($1 AS INT), $2) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText(NULL)),
    (2, ST_GeomFromText('POINT EMPTY')),
    (3, ST_GeomFromText('POINT (0 0)')),
    (4, ST_GeomFromText('POINT (1 0)')),
    (5, ST_GeomFromText('POINT (10 0)')),
    (6, ST_GeomFromText('POINT (100 0)')),
    (7, ST_GeomFromText('POINT (101 0)')),
    (8, ST_GeomFromText('POINT (0 0)'))
) AS t(id, geom)
ORDER BY id
