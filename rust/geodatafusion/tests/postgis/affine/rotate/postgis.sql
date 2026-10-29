SELECT ST_AsEWKB(ST_Rotate(ST_GeomFromText($1), $2)) AS rotate_geom,
       ST_AsEWKB(ST_Rotate(ST_GeomFromText($1), $2, $3, $4)) AS rotate_origin_xy_geom,
       ST_AsEWKB(ST_Rotate(ST_GeomFromText($1), $2, ST_GeomFromText($5))) AS rotate_origin_point_geom
