SELECT ST_AsBinary(ST_BoundingDiagonal(ST_GeomFromText(input.wkt), input.fits)) AS geom
FROM (VALUES ($1, $2)) AS input(wkt, fits)
