SELECT ST_GeometryType(p.geom) AS geom_type, ST_NPoints(p.geom)::float8 AS npoints FROM (SELECT ST_GeneratePoints(ST_GeomFromText($1::text), CAST(CAST($2 AS bigint) AS integer)) AS geom) p
