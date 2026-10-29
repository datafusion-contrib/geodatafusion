SELECT ST_AsTWKB(
  ARRAY[ST_GeomFromText($1), ST_GeomFromText($2)],
  ARRAY[$3::bigint, $4::bigint],
  ($5::bigint)::int,
  ($6::bigint)::int,
  ($7::bigint)::int,
  $8::boolean,
  $9::boolean
) AS twkb
