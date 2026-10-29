SELECT ST_AsBinary(ST_LineExtend(ST_GeomFromText($1), $2, $3)) AS line_extend,
       ST_AsBinary(ST_QuantizeCoordinates(ST_GeomFromText($1), $4)) AS quantize,
       ST_AsBinary(ST_RemoveRepeatedPoints(ST_GeomFromText($1), $5)) AS remove_repeated,
       ST_AsBinary(ST_Segmentize(ST_GeomFromText($1), $2)) AS segmentize,
       ST_AsBinary(ST_ShiftLongitude(ST_GeomFromText($1))) AS shift_longitude,
       ST_AsBinary(ST_WrapX(ST_GeomFromText($1), $6, $7)) AS wrap_x
