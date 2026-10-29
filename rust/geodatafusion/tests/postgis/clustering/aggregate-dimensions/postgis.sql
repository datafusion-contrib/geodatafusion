SELECT array_agg(encode(ST_AsBinary(cluster), 'hex')
                 ORDER BY encode(ST_AsBinary(cluster), 'hex')) AS clusters
FROM (
    SELECT unnest(ST_ClusterWithin(geom, $1::float8 ORDER BY id)) AS cluster
    FROM (VALUES
        (1, ST_GeomFromText('POINT M (0 0 5)')),
        (2, ST_GeomFromText('POINT M (0.1 0 6)')),
        (3, ST_GeomFromText('POINT Z (1000 0 5)')),
        (4, ST_GeomFromText('POINT Z (1000.1 0 6)')),
        (5, ST_GeomFromText('POLYGON Z ((1000000 0 1, 1000001 0 2, 1000001 1 3, 1000000 0 1))'))
    ) AS t(id, geom)
) AS u
