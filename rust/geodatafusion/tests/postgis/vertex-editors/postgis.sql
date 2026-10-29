SELECT ST_AsEWKB(ST_AddPoint(ST_GeomFromText($1::text), ST_GeomFromText($2::text), $3::float8::int)) AS add_point,
       ST_AsEWKB(ST_SetPoint(ST_GeomFromText($1::text), $3::float8::int, ST_GeomFromText($5::text))) AS set_point,
       ST_AsEWKB(ST_RemovePoint(ST_GeomFromText($1::text), $3::float8::int)) AS remove_point,
       ST_AsEWKB(ST_Reverse(ST_GeomFromText($4::text))) AS reverse,
       ST_AsEWKB(ST_Scroll(ST_GeomFromText($1::text), ST_GeomFromText($2::text))) AS scroll
