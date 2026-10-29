SELECT ST_AsEWKB(ST_Boundary(ST_GeomFromEWKT($1))) AS geom
