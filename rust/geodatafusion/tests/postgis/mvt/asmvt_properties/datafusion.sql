SELECT ST_AsMVT(
         named_struct(
           'id', $1,
           'i8', $2,
           'f8', $3,
           'flag', $4,
           'label', $5,
           'geom', ST_GeomFromText($6)
         ),
         'layer',
         4096,
         'geom',
         'id'
       ) AS tile
