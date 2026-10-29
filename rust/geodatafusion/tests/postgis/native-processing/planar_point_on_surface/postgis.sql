SELECT ST_AsEWKB(ST_PointOnSurface(ST_Force2D(ST_GeomFromText($1)))) AS geom
