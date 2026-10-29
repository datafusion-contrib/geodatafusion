SELECT ST_Intersects(circle.center, ST_ConvexHull(geom)) AS center_in_boundary,
       ST_Intersects(circle.nearest, geom) AS witness_is_obstacle,
       abs(circle.radius - ST_Distance(circle.center, circle.nearest)) <= 1e-12 AS radius_matches_witness,
       ST_Distance(circle.center, geom) >= circle.radius - 1e-12 AS circle_is_empty,
       circle.radius >= $3 - $4 AS radius_within_optimum_tolerance,
       circle.radius <= $3 + 1e-12 AS radius_within_upper_bound
FROM (SELECT ST_LargestEmptyCircle(ST_GeomFromText($1), $2) AS circle,
             ST_GeomFromText($1) AS geom)
