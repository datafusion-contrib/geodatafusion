SELECT extent.xmin AS xmin,
       extent.ymin AS ymin,
       extent.xmax AS xmax,
       extent.ymax AS ymax
FROM (
  SELECT ST_Extent(ST_GeomFromText(input.wkt)) AS extent
  FROM (VALUES ($1), ($2)) AS input(wkt)
) AS aggregate
