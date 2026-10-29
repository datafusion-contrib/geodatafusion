SELECT ST_AsEWKB(ST_InteriorRingN(ST_GeomFromText($1), (($2)::text)::integer)) AS ring
