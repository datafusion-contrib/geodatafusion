SELECT ST_Scale(ST_GeomFromText($1), $2, $3) AS scale_xy_geom,
       ST_Scale(ST_GeomFromText($1), $2, $3, $4) AS scale_xyz_geom,
       ST_Scale(ST_GeomFromText($1), ST_GeomFromText($5)) AS scale_factor_geom,
       ST_Scale(ST_GeomFromText($1), ST_GeomFromText($5), ST_GeomFromText($6)) AS scale_factor_origin_geom
