SELECT ST_AsEWKB(ST_PointFromGeoHash($1, CAST(CAST($2 AS bigint) AS integer))) AS geom
