SELECT ST_XMin(extent) AS xmin,
       ST_YMin(extent) AS ymin,
       ST_ZMin(extent) AS zmin,
       ST_XMax(extent) AS xmax,
       ST_YMax(extent) AS ymax,
       ST_ZMax(extent) AS zmax
FROM (
  SELECT ST_3DExtent(ST_GeomFromText(input.wkt)) AS extent
  FROM (VALUES ($1::text), ($2::text)) AS input(wkt)
) AS aggregate
