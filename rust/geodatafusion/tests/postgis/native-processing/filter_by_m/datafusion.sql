SELECT ST_FilterByM(ST_GeomFromText($1), $2, $3, $4) AS geom
