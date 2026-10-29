SELECT ST_AsEWKB(ST_Collect(ST_GeomFromText($1))) AS geometry,
       ST_Collect(ST_GeomFromText($1)) IS NULL AS is_null
