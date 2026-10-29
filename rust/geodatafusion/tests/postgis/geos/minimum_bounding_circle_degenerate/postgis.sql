SELECT ST_AsEWKB(ST_MinimumBoundingCircle(ST_GeomFromEWKT($1), 1)) AS geom
