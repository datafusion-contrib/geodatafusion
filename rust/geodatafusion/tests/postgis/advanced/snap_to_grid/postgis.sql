SELECT
    ST_AsEWKB(ST_SnapToGrid(ST_GeomFromText($1), $2::float8)) AS xy,
    ST_AsEWKB(ST_SnapToGrid(ST_GeomFromText($1), $3::float8, $4::float8)) AS xy_sizes,
    ST_AsEWKB(ST_SnapToGrid(ST_GeomFromText($1), $5::float8, $6::float8, $3::float8, $4::float8)) AS origin_xy,
    ST_AsEWKB(ST_SnapToGrid(ST_GeomFromText($1), ST_GeomFromText($7), $3::float8, $4::float8, $2::float8, $2::float8)) AS point_origin
