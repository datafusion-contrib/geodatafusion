SELECT ST_GeometryType(ST_Letters($1)) AS geom_type, ST_IsEmpty(ST_Letters($1)) AS is_empty, CAST(ST_NDims(ST_Letters($1)) AS DOUBLE) AS ndims, (ST_NPoints(ST_Letters($1)) > 0) AS has_points
