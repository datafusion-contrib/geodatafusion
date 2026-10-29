SELECT ST_Translate(ST_GeomFromText($1), $2, $3) AS translate_xy_geom,
       ST_Translate(ST_GeomFromText($1), $2, $3, $4) AS translate_xyz_geom
