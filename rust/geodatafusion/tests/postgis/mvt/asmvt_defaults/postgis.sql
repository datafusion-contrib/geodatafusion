SELECT ST_AsMVT(t, NULL, NULL, NULL, NULL) AS tile
FROM (
  SELECT $1::bigint AS id,
         $2::text AS name,
         ST_GeomFromText($3::text) AS geom
) t
