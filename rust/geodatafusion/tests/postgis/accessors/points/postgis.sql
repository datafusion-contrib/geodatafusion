SELECT ST_AsEWKB(ST_Points(ST_GeomFromText($1::text))) AS points
