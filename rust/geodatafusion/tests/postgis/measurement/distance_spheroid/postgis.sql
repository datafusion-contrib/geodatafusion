SELECT ST_DistanceSpheroid(ST_GeomFromText($1), ST_GeomFromText($2), $3::text::spheroid) AS value
