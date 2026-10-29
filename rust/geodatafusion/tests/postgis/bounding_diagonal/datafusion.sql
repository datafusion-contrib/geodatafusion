SELECT ST_AsBinary(ST_BoundingDiagonal(ST_GeomFromText(input.wkt))) AS geom
FROM (VALUES ($1)) AS input(wkt)
