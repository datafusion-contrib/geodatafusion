SELECT i::float8 AS i, j::float8 AS j, ST_AsEWKB(geom) AS geom
FROM ST_HexagonGrid(1.0, ST_MakeEnvelope(-1, -1, 1, 1)) AS grid
WHERE $1::text IS NULL OR $1::text IS NOT NULL
ORDER BY i, j
