SELECT ST_DistanceSphere(ST_GeomFromText($1), ST_GeomFromText($2), $3::float8) AS value
