SELECT i::float8 AS i, j::float8 AS j, ST_AsEWKB(geom) AS geom
FROM ST_SquareGrid(0.5, ST_PointFromText('POINT ZM(-0.25 0.25 1 2)', 4326)) AS grid
WHERE $1::text IS NULL OR $1::text IS NOT NULL
ORDER BY i, j
