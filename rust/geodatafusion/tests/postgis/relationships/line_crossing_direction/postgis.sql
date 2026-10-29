SELECT CAST(ST_LineCrossingDirection(ST_GeomFromText($1), ST_GeomFromText($2)) AS text) AS value,
       ST_LineCrossingDirection(ST_GeomFromText($1), ST_GeomFromText($2)) IS NULL AS is_null
