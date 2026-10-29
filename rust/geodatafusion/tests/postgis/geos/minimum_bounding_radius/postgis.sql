SELECT ST_X((ST_MinimumBoundingRadius(ST_GeomFromText($1))).center) AS center_x,
       ST_Y((ST_MinimumBoundingRadius(ST_GeomFromText($1))).center) AS center_y,
       (ST_MinimumBoundingRadius(ST_GeomFromText($1))).radius AS radius
