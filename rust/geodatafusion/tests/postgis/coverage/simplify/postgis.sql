SELECT ST_AsEWKB(ST_CoverageSimplify(ST_GeomFromText(wkt), tolerance, simplify_boundary)
       OVER (PARTITION BY grp ORDER BY id)) AS geom
FROM (
  SELECT 1 AS id, 1 AS grp, $1 AS wkt, $4::double precision AS tolerance, $7::boolean AS simplify_boundary
  UNION ALL SELECT 2, 1, $2, $5::double precision, $8::boolean
  UNION ALL SELECT 3, 2, $3, $6::double precision, $9::boolean
) AS input
ORDER BY id
