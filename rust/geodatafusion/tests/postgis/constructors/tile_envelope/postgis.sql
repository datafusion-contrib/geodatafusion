SELECT ST_AsEWKB(ST_TileEnvelope(CAST(CAST($1 AS bigint) AS integer), CAST(CAST($2 AS bigint) AS integer), CAST(CAST($3 AS bigint) AS integer))) AS tile_default,
       ST_AsEWKB(ST_TileEnvelope(
           CAST(CAST($1 AS bigint) AS integer),
           CAST(CAST($2 AS bigint) AS integer),
           CAST(CAST($3 AS bigint) AS integer),
           ST_SetSRID(ST_GeomFromText('LINESTRING(-20037508.342789244 -20037508.342789244,20037508.342789244 20037508.342789244)'), 3857),
           CAST($4 AS double precision)
       )) AS tile_margin
