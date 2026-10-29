SELECT ST_Contains(ST_GeomFromText($1), ST_GeomFromText($2)) AS value
