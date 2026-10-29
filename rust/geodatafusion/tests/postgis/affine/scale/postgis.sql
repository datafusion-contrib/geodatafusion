SELECT ST_AsEWKB(ST_Scale(ST_GeomFromText($1), CAST($2 AS double precision), CAST($3 AS double precision))) AS scale_xy_geom,
       ST_AsEWKB(ST_Scale(ST_GeomFromText($1), CAST($2 AS double precision), CAST($3 AS double precision), CAST($4 AS double precision))) AS scale_xyz_geom,
       ST_AsEWKB(ST_Scale(ST_GeomFromText($1), ST_GeomFromText($5))) AS scale_factor_geom,
       ST_AsEWKB(ST_Scale(ST_GeomFromText($1), ST_GeomFromText($5), ST_GeomFromText($6))) AS scale_factor_origin_geom
