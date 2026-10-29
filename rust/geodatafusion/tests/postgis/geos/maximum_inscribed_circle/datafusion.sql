SELECT ST_X(circle.center) AS center_x,
       ST_Y(circle.center) AS center_y,
       ST_X(circle.nearest) AS nearest_x,
       ST_Y(circle.nearest) AS nearest_y,
       circle.radius AS radius
FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText($1)) AS circle)
