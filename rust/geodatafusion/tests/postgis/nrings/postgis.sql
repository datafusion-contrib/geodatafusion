SELECT ST_NRings(ST_GeomFromText($1))::double precision AS value
