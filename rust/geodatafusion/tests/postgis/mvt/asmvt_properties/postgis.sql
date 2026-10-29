SELECT ST_AsMVT(t, 'layer', 4096, 'geom', 'id') AS tile
FROM (
  SELECT $1::bigint AS id,
         $2::bigint AS i8,
         $3::float8 AS f8,
         $4::boolean AS flag,
         $5::text AS label,
         ST_GeomFromText($6::text) AS geom
) t
