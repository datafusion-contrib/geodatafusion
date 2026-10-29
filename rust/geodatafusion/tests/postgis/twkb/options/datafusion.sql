SELECT ST_AsTWKB(
  ST_GeomFromText($1),
  $2,
  $3,
  $4,
  $5,
  $6
) AS twkb
