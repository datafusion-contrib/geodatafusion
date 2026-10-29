SELECT ST_Within(circle.center, geom) AS center_inside,
       ST_Intersects(circle.nearest, ST_ExteriorRing(geom)) AS witness_on_boundary,
       abs(circle.radius - ST_Distance(circle.center, circle.nearest)) <= 1e-12 AS radius_matches_witness,
       abs(ST_X(circle.center) - $2) <= $5 AS center_x_within_tolerance,
       abs(ST_Y(circle.center) - $3) <= $5 AS center_y_within_tolerance,
       abs(circle.radius - $4) <= $5 AS radius_within_tolerance
FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText($1)) AS circle,
             ST_GeomFromText($1) AS geom)
