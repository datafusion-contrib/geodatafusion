SELECT ST_AsEWKB(ST_ClipByBox2D(
    ST_GeomFromText($1),
    ST_MakeBox2D(ST_Point($2, $3), ST_Point($4, $5))
)) AS geom
