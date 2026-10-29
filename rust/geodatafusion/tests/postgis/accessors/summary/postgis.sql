SELECT regexp_replace(ST_Summary(ST_GeomFromText($1::text)), 'B', '') AS summary
