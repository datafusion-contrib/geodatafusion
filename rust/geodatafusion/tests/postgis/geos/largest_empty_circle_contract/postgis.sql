SELECT ST_Intersects((c.circle).center, ST_ConvexHull(c.geom)) AS center_in_boundary,
       ST_Intersects((c.circle).nearest, c.geom) AS witness_is_obstacle,
       abs((c.circle).radius - ST_Distance((c.circle).center, (c.circle).nearest)) <= 1e-12 AS radius_matches_witness,
       ST_Distance((c.circle).center, c.geom) >= (c.circle).radius - 1e-12 AS circle_is_empty,
       (c.circle).radius >= $3::float8 - $4::float8 AS radius_within_optimum_tolerance,
       (c.circle).radius <= $3::float8 + 1e-12 AS radius_within_upper_bound
FROM (SELECT ST_LargestEmptyCircle(ST_GeomFromText($1), $2::float8) AS circle,
             ST_GeomFromText($1) AS geom) AS c
