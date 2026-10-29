SELECT id, ST_ClusterDBSCAN(geom, $1::float8, $2::int4) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('POINT (0 0)')),
    (2, ST_GeomFromText('POINT (-0.9 0)')),
    (3, ST_GeomFromText('POINT (-0.89 0)')),
    (4, ST_GeomFromText('POINT (-0.88 0)')),
    (5, ST_GeomFromText('POINT (1.9 0)')),
    (6, ST_GeomFromText('POINT (2.8 0)')),
    (7, ST_GeomFromText('POINT (2.79 0)')),
    (8, ST_GeomFromText('POINT (2.78 0)')),
    (9, ST_GeomFromText('POINT (0.95 0)'))
) AS t(id, geom)
ORDER BY id
