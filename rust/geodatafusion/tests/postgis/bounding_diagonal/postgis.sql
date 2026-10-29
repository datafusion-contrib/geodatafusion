SELECT ST_AsEWKB(ST_BoundingDiagonal(ST_GeomFromText(input.wkt))) AS geom
FROM (VALUES ($1::text)) AS input(wkt)
