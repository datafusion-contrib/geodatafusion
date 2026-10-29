SELECT ST_AsTWKB(
  ARRAY[ST_GeomFromText($1), ST_GeomFromText($2)],
  ARRAY[$3, $4],
  $5,
  $6,
  $7,
  $8,
  $9
) AS twkb
