SELECT array_agg(encode(ST_AsEWKB(cluster), 'hex')
                 ORDER BY encode(ST_AsEWKB(cluster), 'hex')) AS clusters
FROM (
    SELECT unnest(ST_ClusterWithin(geom, $1::float8 ORDER BY id)) AS cluster
    FROM (VALUES
        (1, ST_GeomFromText('POINT (0 0)')),
        (2, ST_GeomFromText('POINT (0.5 0)')),
        (3, ST_GeomFromText('POINT (10 0)')),
        (4, ST_GeomFromText('POINT (10.5 0)')),
        (5, NULL),
        (6, ST_GeomFromText('POLYGON EMPTY'))
    ) AS t(id, geom)
) AS u
