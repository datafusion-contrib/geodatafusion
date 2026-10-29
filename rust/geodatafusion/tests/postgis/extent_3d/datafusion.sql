SELECT extent.xmin AS xmin,
       extent.ymin AS ymin,
       extent.zmin AS zmin,
       extent.xmax AS xmax,
       extent.ymax AS ymax,
       extent.zmax AS zmax
FROM (
  SELECT ST_3DExtent(ST_GeomFromText(input.wkt)) AS extent
  FROM (VALUES ($1), ($2)) AS input(wkt)
) AS aggregate
