SELECT ST_GeometryType(p.geom) AS geom_type, CAST(ST_NPoints(p.geom) AS DOUBLE) AS npoints FROM (SELECT ST_GeneratePoints(ST_GeomFromText($1), $2) AS geom) p
