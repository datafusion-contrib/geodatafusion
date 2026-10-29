SELECT ST_AsKML(ST_GeomFromEWKT($1), CAST(CAST($2 AS bigint) AS integer), $3) AS kml
