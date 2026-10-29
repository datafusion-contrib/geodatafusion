SELECT ST_CoveredBy(ST_GeomFromText($1), ST_GeomFromText($2)) AS value
