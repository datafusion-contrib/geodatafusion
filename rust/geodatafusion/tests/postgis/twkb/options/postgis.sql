SELECT ST_AsTWKB(
  ST_GeomFromText($1),
  ($2::bigint)::int,
  ($3::bigint)::int,
  ($4::bigint)::int,
  $5::boolean,
  $6::boolean
) AS twkb
