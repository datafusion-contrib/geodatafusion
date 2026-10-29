SELECT CAST(id AS INT) AS id, ST_ClusterIntersectingWin(geom) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('LINESTRING (0 0, 1 1)')),
    (2, ST_GeomFromText('LINESTRING (5 5, 4 4)')),
    (3, ST_GeomFromText(NULL)),
    (4, ST_GeomFromText('LINESTRING (0 0, -1 -1)')),
    (5, ST_GeomFromText('LINESTRING (6 6, 7 7)')),
    (6, ST_GeomFromText('POLYGON EMPTY')),
    (7, ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')),
    (8, ST_GeomFromText('POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0), (12 2, 12 8, 18 8, 18 2, 12 2))')),
    (9, ST_GeomFromText('POINT (14 5)')),
    (10, ST_GeomFromText('POINT (10 5)'))
) AS t(id, geom)
WHERE $1 IS NOT NULL
ORDER BY id
