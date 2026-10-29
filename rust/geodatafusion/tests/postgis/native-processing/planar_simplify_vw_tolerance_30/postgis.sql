SELECT ST_AsEWKB(ST_SimplifyVW(ST_Force2D(ST_GeomFromText($1)), 30.0)) AS geom
