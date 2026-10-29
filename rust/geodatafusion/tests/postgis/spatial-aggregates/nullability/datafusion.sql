SELECT ST_AsEWKB(ST_Collect(arg1)) AS geometry,
       ST_Collect(arg1) IS NULL AS is_null
FROM oracle_input
