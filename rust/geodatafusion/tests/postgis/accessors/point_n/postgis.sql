SELECT ST_AsEWKB(ST_PointN(ST_GeomFromText($1), (($2)::text)::integer)) AS point
