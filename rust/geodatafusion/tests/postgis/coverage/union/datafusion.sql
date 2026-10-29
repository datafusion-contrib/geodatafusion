SELECT ST_CoverageUnion(g) AS geom
FROM (
  SELECT ST_GeomFromText(input.wkt) AS g
  FROM (VALUES ($1), ($2)) AS input(wkt)
) AS source
