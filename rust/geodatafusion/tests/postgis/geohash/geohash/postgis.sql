SELECT ST_GeoHash(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer)) AS geohash
