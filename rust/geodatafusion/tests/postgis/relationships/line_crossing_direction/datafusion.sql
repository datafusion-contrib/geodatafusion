SELECT CAST(ST_LineCrossingDirection(ST_GeomFromText(arg1), ST_GeomFromText(arg2)) AS text) AS value,
       ST_LineCrossingDirection(ST_GeomFromText(arg1), ST_GeomFromText(arg2)) IS NULL AS is_null
FROM oracle_input
