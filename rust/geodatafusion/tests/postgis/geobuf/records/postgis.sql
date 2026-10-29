SELECT ST_AsGeobuf(t) AS geobuf
FROM (
  SELECT ST_GeomFromText($1) AS geom,
         $2::text AS name,
         $3::int8 AS n,
         $4::text AS nullable
) AS t
