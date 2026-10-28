SELECT ST_XMin(extent) AS xmin,
       ST_YMin(extent) AS ymin,
       ST_XMax(extent) AS xmax,
       ST_YMax(extent) AS ymax
FROM (
  SELECT ST_Extent(ST_GeomFromText(input.wkt)) AS extent
  FROM (VALUES ($1::text), ($2::text)) AS input(wkt)
) AS aggregate
