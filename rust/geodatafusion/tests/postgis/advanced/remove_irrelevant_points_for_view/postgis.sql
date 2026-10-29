SELECT ST_AsEWKB(
    ST_RemoveIrrelevantPointsForView(
        ST_GeomFromText($1),
        ST_MakeBox2D(ST_Point($2::float8, $3::float8), ST_Point($4::float8, $5::float8)),
        $6::boolean
    )
) AS geom
