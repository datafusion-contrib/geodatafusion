SELECT ST_Within((c.circle).center, c.geom) AS center_inside,
       ST_Intersects((c.circle).nearest, ST_ExteriorRing(c.geom)) AS witness_on_boundary,
       abs((c.circle).radius - ST_Distance((c.circle).center, (c.circle).nearest)) <= 1e-12 AS radius_matches_witness,
       abs(ST_X((c.circle).center) - $2::float8) <= $5::float8 AS center_x_within_tolerance,
       abs(ST_Y((c.circle).center) - $3::float8) <= $5::float8 AS center_y_within_tolerance,
       abs((c.circle).radius - $4::float8) <= $5::float8 AS radius_within_tolerance
FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText($1)) AS circle,
             ST_GeomFromText($1) AS geom) AS c
