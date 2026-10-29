SELECT ST_X(projected.geom) AS x,
       ST_Y(projected.geom) AS y
FROM (
    SELECT ST_Project(ST_GeomFromEWKT($1), ST_GeomFromEWKT($2), $3::DOUBLE) AS geom
) AS projected
