SELECT ST_FrechetDistance(ST_GeomFromText($1), ST_GeomFromText($2), $3) AS value
