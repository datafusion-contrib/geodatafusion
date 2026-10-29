SELECT ST_AsEWKB(ST_LongestLine(ST_GeomFromText($1), ST_GeomFromText($2))) AS value
