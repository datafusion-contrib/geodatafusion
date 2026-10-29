SELECT ST_AsEWKB(ST_FilterByM(ST_GeomFromText($1::text), $2::float8, $3::float8, CAST($4 AS boolean))) AS geom
