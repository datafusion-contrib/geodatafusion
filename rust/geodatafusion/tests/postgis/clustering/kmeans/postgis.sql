SELECT id, ST_ClusterKMeans(geom, $1::int4, $2::float8) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, NULL),
    (2, ST_GeomFromText('POINT EMPTY')),
    (3, ST_GeomFromText('POINT (0 0)')),
    (4, ST_GeomFromText('POINT (1 0)')),
    (5, ST_GeomFromText('POINT (10 0)')),
    (6, ST_GeomFromText('POINT (100 0)')),
    (7, ST_GeomFromText('POINT (101 0)')),
    (8, ST_GeomFromText('POINT (0 0)'))
) AS t(id, geom)
ORDER BY id
