SELECT ST_AsEWKB(ST_GeomFromGML($1, CAST(CAST($2 AS bigint) AS integer))) AS geom
