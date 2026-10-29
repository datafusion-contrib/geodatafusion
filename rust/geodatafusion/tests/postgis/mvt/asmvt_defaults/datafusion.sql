SELECT ST_AsMVT(
         named_struct(
           'id', $1,
           'name', $2,
           'geom', ST_GeomFromText($3)
         ),
         CAST(NULL AS VARCHAR),
         CAST(NULL AS BIGINT),
         CAST(NULL AS VARCHAR),
         CAST(NULL AS VARCHAR)
       ) AS tile
