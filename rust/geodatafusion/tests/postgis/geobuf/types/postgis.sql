SELECT ST_AsGeobuf(t, 'geom') AS geobuf
FROM (
  SELECT ST_GeomFromText($1) AS geom,
         $2::text AS name,
         $3::int8 AS n,
         $4::boolean AS flag,
         $5::float8 AS value,
         $6::text AS nullable
) AS t
