SELECT ST_CoverageInvalidEdges(ST_GeomFromText(wkt), tolerance)
       OVER (PARTITION BY grp ORDER BY id) AS geom
FROM (
  SELECT 1 AS id, 1 AS grp, $1 AS wkt, $4 AS tolerance
  UNION ALL SELECT 2, 1, $2, $5
  UNION ALL SELECT 3, 2, $3, $6
) AS input
ORDER BY id
