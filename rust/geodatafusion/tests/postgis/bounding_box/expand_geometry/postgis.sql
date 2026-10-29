SELECT ST_AsEWKB(ST_Expand(ST_GeomFromText($1::text), $2::float8, $3::float8, $4::float8)) AS geom
