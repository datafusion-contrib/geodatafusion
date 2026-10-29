SELECT ST_AsEWKB(ST_CollectionExtract(ST_GeomFromText($1), CAST(CAST($2 AS double precision) AS int))) AS geom
