SELECT ST_X(bounds.center) AS center_x,
       ST_Y(bounds.center) AS center_y,
       bounds.radius AS radius
FROM (SELECT ST_MinimumBoundingRadius(ST_GeomFromText($1)) AS bounds)
