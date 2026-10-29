SELECT ST_TileEnvelope($1, $2, $3) AS tile_default,
       ST_TileEnvelope($1, $2, $3, $4) AS tile_margin
