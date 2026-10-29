SELECT ST_AsEWKB(ST_ChaikinSmoothing(ST_GeomFromText($1::text), CAST(CAST($2 AS bigint) AS integer), CAST($3 AS boolean))) AS geom
