SELECT ST_Intersects(ST_GeomFromText($1), ST_GeomFromText($2)) AS value
