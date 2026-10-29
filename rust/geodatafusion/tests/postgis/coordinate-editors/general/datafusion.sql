SELECT ST_AsBinary(ST_QuantizeCoordinates(ST_GeomFromText($1), $2)) AS quantize,
       ST_AsBinary(ST_RemoveRepeatedPoints(ST_GeomFromText($1), $3)) AS remove_repeated,
       ST_AsBinary(ST_Segmentize(ST_GeomFromText($1), $4)) AS segmentize,
       ST_AsBinary(ST_ShiftLongitude(ST_GeomFromText($1))) AS shift_longitude,
       ST_AsBinary(ST_WrapX(ST_GeomFromText($1), $5, $6)) AS wrap_x
