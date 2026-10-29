SELECT ST_AsEWKB(ST_Centroid(ST_GeomFromText($1))) AS geom
