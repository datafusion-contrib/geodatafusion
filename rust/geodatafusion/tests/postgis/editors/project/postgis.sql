SELECT ST_X(projected.geom)::float8 AS x,
       ST_Y(projected.geom)::float8 AS y
FROM (
    SELECT ST_Project(ST_GeomFromEWKT($1), $2::float8, $3::float8) AS geom
) AS projected
