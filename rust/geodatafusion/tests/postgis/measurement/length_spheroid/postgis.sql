SELECT ST_LengthSpheroid(ST_GeomFromText($1), $2::text::spheroid) AS value
