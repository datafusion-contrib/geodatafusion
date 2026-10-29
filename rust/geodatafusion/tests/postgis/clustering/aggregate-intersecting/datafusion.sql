SELECT ST_ClusterIntersecting(geom ORDER BY id) AS clusters
FROM (VALUES
    (1, ST_GeomFromText('LINESTRING (0 0, 1 1)')),
    (2, ST_GeomFromText('LINESTRING (1 1, 2 2)')),
    (3, ST_GeomFromText('LINESTRING (5 5, 6 6)')),
    (4, ST_GeomFromText(NULL)),
    (5, ST_GeomFromText('POLYGON EMPTY')),
    (6, ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')),
    (7, ST_GeomFromText('POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0), (12 2, 12 8, 18 8, 18 2, 12 2))')),
    (8, ST_GeomFromText('POINT (14 5)')),
    (9, ST_GeomFromText('POINT (10 5)'))
) AS t(id, geom)
WHERE $1 IS NOT NULL
