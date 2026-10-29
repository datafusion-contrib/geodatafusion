SELECT ST_X((ST_MaximumInscribedCircle(ST_GeomFromText($1))).center) AS center_x,
       ST_Y((ST_MaximumInscribedCircle(ST_GeomFromText($1))).center) AS center_y,
       ST_X((ST_MaximumInscribedCircle(ST_GeomFromText($1))).nearest) AS nearest_x,
       ST_Y((ST_MaximumInscribedCircle(ST_GeomFromText($1))).nearest) AS nearest_y,
       (ST_MaximumInscribedCircle(ST_GeomFromText($1))).radius AS radius
