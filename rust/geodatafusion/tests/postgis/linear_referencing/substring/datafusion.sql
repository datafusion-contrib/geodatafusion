SELECT ST_AsBinary(ST_LineSubstring(ST_GeomFromText($1), $2, $3)) AS result
