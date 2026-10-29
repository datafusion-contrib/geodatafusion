SELECT id, ST_ClusterWithinWin(geom, $1::float8) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('LINESTRING (0 0, 1 1)')),
    (2, ST_GeomFromText('LINESTRING (5 5, 4 4)')),
    (3, NULL),
    (4, ST_GeomFromText('LINESTRING (0 0, -1 -1)')),
    (5, ST_GeomFromText('LINESTRING (6 6, 7 7)')),
    (6, ST_GeomFromText('POLYGON EMPTY')),
    (7, ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')),
    (8, ST_GeomFromText('LINESTRING (1 1, 2 2)')),
    (9, ST_GeomFromText('LINESTRING (2 2, 3 3)'))
) AS t(id, geom)
ORDER BY id
