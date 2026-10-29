SELECT ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer)) IS NULL AS is_null
