SELECT ST_X(ST_GeomFromText($1)) AS x,
       ST_Y(ST_GeomFromText($1)) AS y,
       ST_Z(ST_GeomFromText($1)) AS z,
       ST_M(ST_GeomFromText($1)) AS m
