SELECT ST_AsMVT(
         named_struct(
           'id', id,
           'name', name,
           'geom', geom
         ),
         'layer',
         4096,
         'geom',
         'id'
       ) AS tile
FROM (
  SELECT $1 AS id, $2 AS name, ST_GeomFromText($3) AS geom
  UNION ALL
  SELECT $4 AS id, $5 AS name, ST_GeomFromText($6) AS geom
  UNION ALL
  SELECT $7 AS id, $8 AS name, ST_GeomFromText($9) AS geom
) t
