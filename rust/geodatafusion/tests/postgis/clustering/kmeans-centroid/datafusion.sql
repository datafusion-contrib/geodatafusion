SELECT CAST(id AS INT) AS id, ST_ClusterKMeans(geom, CAST($1 AS INT), $2) OVER (ORDER BY id) AS cid
FROM (VALUES
    (1, ST_GeomFromText('LINESTRING (0 0, 10 0)')),
    (2, ST_GeomFromText('LINESTRING (100 0, 110 0)')),
    (3, ST_GeomFromText('POLYGON ((200 0, 210 0, 210 10, 200 10, 200 0))'))
) AS t(id, geom)
ORDER BY id
