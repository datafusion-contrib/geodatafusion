SELECT ST_AsEWKB(ST_SetEffectiveArea(ST_GeomFromText($1::text), $2::float8, CAST(CAST($3 AS bigint) AS integer))) AS geom
