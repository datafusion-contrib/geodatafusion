SELECT id, ST_ClusterKMeans(geom, $1::int4, $2::float8) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('POINT Z (0 0 1)')),
    (2, ST_GeomFromText('POINT Z (0 0 5)')),
    (3, ST_GeomFromText('POINT Z (0 0 7)')),
    (4, ST_GeomFromText('LINESTRING Z (0 0 1, 0 0 -1)'))
) AS t(id, geom)
ORDER BY id
