SELECT ST_CoverageSimplify(ST_GeomFromText(wkt), tolerance, simplify_boundary)
       OVER (PARTITION BY grp ORDER BY id) AS geom
FROM (
  SELECT 1 AS id, 1 AS grp, $1 AS wkt, $4 AS tolerance, $7 AS simplify_boundary
  UNION ALL SELECT 2, 1, $2, $5, $8
  UNION ALL SELECT 3, 2, $3, $6, $9
) AS input
ORDER BY id
