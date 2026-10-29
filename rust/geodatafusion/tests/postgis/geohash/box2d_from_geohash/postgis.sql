SELECT ST_AsEWKB(ST_Box2dFromGeoHash($1, CAST(CAST($2 AS bigint) AS integer))::geometry) AS geom
