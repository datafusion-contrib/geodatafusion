SELECT ST_AsEWKB(ST_OffsetCurve(ST_GeomFromText($1), $2, $3)) AS geom
