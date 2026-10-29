SELECT ST_AsMVT(t, 'layer', 4096, 'geom', 'id') AS tile
FROM (
  SELECT $1::bigint AS id, $2::text AS name, ST_GeomFromText($3::text) AS geom
  UNION ALL
  SELECT $4::bigint AS id, $5::text AS name, ST_GeomFromText($6::text) AS geom
  UNION ALL
  SELECT $7::bigint AS id, $8::text AS name, ST_GeomFromText($9::text) AS geom
) t
