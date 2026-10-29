SELECT ST_ClusterWithin(geom, $1 ORDER BY id) AS clusters
FROM (VALUES
    (1, ST_GeomFromText('POINT (0 0)')),
    (2, ST_GeomFromText('POINT (0.5 0)')),
    (3, ST_GeomFromText('POINT (10 0)')),
    (4, ST_GeomFromText('POINT (10.5 0)')),
    (5, ST_GeomFromText(NULL)),
    (6, ST_GeomFromText('POLYGON EMPTY'))
) AS t(id, geom)
