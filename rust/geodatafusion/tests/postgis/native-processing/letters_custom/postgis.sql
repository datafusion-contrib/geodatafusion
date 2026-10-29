SELECT ST_AsEWKB(ST_Letters($1::text, ($2::text)::json)) AS geom
