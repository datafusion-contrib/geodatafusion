SELECT ST_3DClosestPoint(ST_GeomFromText($1), ST_GeomFromText($2)) AS geom
