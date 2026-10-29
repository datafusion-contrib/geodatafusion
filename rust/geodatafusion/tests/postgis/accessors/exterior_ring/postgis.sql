SELECT ST_AsEWKB(ST_ExteriorRing(ST_GeomFromText($1))) AS ring
