SELECT ST_AsBinary(ST_WrapX(ST_GeomFromText($1), $2, $3)) AS wrap_x
