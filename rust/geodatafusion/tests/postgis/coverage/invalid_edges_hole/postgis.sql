SELECT ST_AsEWKB(ST_CoverageInvalidEdges(ST_GeomFromText(wkt), tolerance)
       OVER (ORDER BY id)) AS geom
FROM (
  SELECT 1 AS id, $1 AS wkt, $5::double precision AS tolerance
  UNION ALL SELECT 2, $2, $6::double precision
  UNION ALL SELECT 3, $3, $7::double precision
  UNION ALL SELECT 4, $4, $8::double precision
) AS input
ORDER BY id
