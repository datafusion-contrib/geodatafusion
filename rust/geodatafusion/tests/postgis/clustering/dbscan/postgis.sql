SELECT id, ST_ClusterDBSCAN(geom, $1::float8, $2::int4) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('POINT (0 0)')),
    (2, ST_GeomFromText('POINT (0 1)')),
    (3, ST_GeomFromText('POINT (-0.5 0.5)')),
    (4, ST_GeomFromText('POINT (1 0)')),
    (5, ST_GeomFromText('POINT (1 1)')),
    (6, ST_GeomFromText('POINT (1.0 0.5)')),
    (7, NULL),
    (8, ST_GeomFromText('POINT EMPTY')),
    (9, ST_GeomFromText('POINT (0 0)')),
    (10, ST_GeomFromText('POINT Z (0 0 99)'))
) AS t(id, geom)
ORDER BY id
