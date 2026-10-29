SELECT ST_AsEWKT(ST_GeomFromEWKT($1), CAST(CAST($2 AS text) AS integer)) AS ewkt
