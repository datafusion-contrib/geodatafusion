SELECT ST_AsEWKB(ST_ShortestLine(ST_GeomFromText($1), ST_GeomFromText($2))) AS value
