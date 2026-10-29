SELECT ST_AsEWKB(ST_MakeLine(ST_GeomFromText($1), ST_GeomFromText($2))) AS geom
