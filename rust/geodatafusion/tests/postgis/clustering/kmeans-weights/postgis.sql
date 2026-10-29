SELECT id, ST_ClusterKMeans(geom, $1::int4, $2::float8) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('POINT M (0 0 1)')),
    (2, ST_GeomFromText('POINT M (1 0 1)')),
    (3, ST_GeomFromText('POINT M (2 0 10000)'))
) AS t(id, geom)
ORDER BY id
