SELECT ST_AsEWKB(ST_RemoveSmallParts(ST_GeomFromText($1), $2::float8, $3::float8)) AS geom
