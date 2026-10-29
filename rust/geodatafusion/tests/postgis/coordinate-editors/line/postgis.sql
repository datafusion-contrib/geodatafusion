SELECT ST_AsEWKB(ST_LineExtend(ST_GeomFromText($1::text), $2::float8, $3::float8)) AS line_extend,
       ST_AsEWKB(ST_QuantizeCoordinates(ST_GeomFromText($1::text), $4::bigint::int)) AS quantize,
       ST_AsEWKB(ST_RemoveRepeatedPoints(ST_GeomFromText($1::text), $5::float8)) AS remove_repeated,
       ST_AsEWKB(ST_Segmentize(ST_GeomFromText($1::text), $2::float8)) AS segmentize,
       ST_AsEWKB(ST_ShiftLongitude(ST_GeomFromText($1::text))) AS shift_longitude,
       ST_AsEWKB(ST_WrapX(ST_GeomFromText($1::text), $6::float8, $7::float8)) AS wrap_x
