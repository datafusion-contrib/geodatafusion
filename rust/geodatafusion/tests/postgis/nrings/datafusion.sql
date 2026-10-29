SELECT CAST(ST_NRings(ST_GeomFromText($1)) AS DOUBLE) AS value
