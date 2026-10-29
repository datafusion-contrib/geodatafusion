SELECT ST_X((ST_LargestEmptyCircle(ST_GeomFromText($1), $2, ST_GeomFromText($3))).center) AS center_x,
       ST_Y((ST_LargestEmptyCircle(ST_GeomFromText($1), $2, ST_GeomFromText($3))).center) AS center_y,
       ST_X((ST_LargestEmptyCircle(ST_GeomFromText($1), $2, ST_GeomFromText($3))).nearest) AS nearest_x,
       ST_Y((ST_LargestEmptyCircle(ST_GeomFromText($1), $2, ST_GeomFromText($3))).nearest) AS nearest_y,
       (ST_LargestEmptyCircle(ST_GeomFromText($1), $2, ST_GeomFromText($3))).radius AS radius
