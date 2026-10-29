SELECT ST_AsEWKB(ST_BoundingDiagonal(ST_GeomFromText(input.wkt), input.fits)) AS geom
FROM (VALUES ($1::text, $2::boolean)) AS input(wkt, fits)
