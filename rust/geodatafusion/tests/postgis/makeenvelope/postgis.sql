SELECT ST_AsEWKB(
  ST_MakeEnvelope(
    $1::text::double precision,
    $2::text::double precision,
    $3::text::double precision,
    $4::text::double precision,
    $5::text::integer
  )
) AS geom
