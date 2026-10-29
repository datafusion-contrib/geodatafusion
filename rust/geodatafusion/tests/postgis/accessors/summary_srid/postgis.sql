SELECT regexp_replace(ST_Summary(ST_GeomFromEWKT($1::text)), 'B', '') AS summary
