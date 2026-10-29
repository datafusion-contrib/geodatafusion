SELECT CAST(i AS DOUBLE) AS i, CAST(j AS DOUBLE) AS j, ST_AsEWKB(geom) AS geom
FROM ST_HexagonGrid(1.0, ST_MakeEnvelope(-1, -1, 1, 1)) AS grid
WHERE $1 IS NULL OR $1 IS NOT NULL
ORDER BY i, j
