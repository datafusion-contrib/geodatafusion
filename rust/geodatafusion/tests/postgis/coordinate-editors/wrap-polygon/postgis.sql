SELECT ST_AsEWKB(ST_WrapX(ST_GeomFromText($1::text), $2::float8, $3::float8)) AS wrap_x
