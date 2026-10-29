SELECT ST_Angle(ST_GeomFromText($1), ST_GeomFromText($2), ST_GeomFromText($3), ST_GeomFromText($4)) AS value
