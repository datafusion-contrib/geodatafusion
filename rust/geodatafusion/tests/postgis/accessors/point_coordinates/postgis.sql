SELECT ST_X(ST_GeomFromText($1::text)) AS x,
       ST_Y(ST_GeomFromText($1::text)) AS y,
       ST_Z(ST_GeomFromText($1::text)) AS z,
       ST_M(ST_GeomFromText($1::text)) AS m
