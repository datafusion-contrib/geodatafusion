SELECT ST_AsEWKB(ST_RotateX(ST_GeomFromText($1), $2)) AS rotatex_geom,
       ST_AsEWKB(ST_RotateY(ST_GeomFromText($1), $2)) AS rotatey_geom,
       ST_AsEWKB(ST_RotateZ(ST_GeomFromText($1), $2)) AS rotatez_geom
