SELECT ST_AsGML(3, ST_GeomFromEWKT($1), CAST(CAST($2 AS bigint) AS integer), CAST(CAST($3 AS bigint) AS integer)) AS gml
