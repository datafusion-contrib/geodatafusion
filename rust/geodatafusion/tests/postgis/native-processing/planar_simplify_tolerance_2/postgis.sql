SELECT ST_AsEWKB(ST_Simplify(ST_Force2D(ST_GeomFromText($1)), 2.0)) AS geom
