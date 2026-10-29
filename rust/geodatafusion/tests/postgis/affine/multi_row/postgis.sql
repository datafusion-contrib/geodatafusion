SELECT ST_AsEWKB(ST_Affine(g, 2,3,4,5,6,7,8,9,10,11,12,13)) AS affine_geom,
       ST_AsEWKB(ST_Rotate(g, 1.5707963267948966)) AS rotate_geom,
       ST_AsEWKB(ST_RotateX(g, 1.5707963267948966)) AS rotatex_geom,
       ST_AsEWKB(ST_RotateY(g, 1.5707963267948966)) AS rotatey_geom,
       ST_AsEWKB(ST_RotateZ(g, 1.5707963267948966)) AS rotatez_geom,
       ST_AsEWKB(ST_Scale(g, 2, 3, 4)) AS scale_geom,
       ST_AsEWKB(ST_Translate(g, 1, 2, 3)) AS translate_geom,
       ST_AsEWKB(ST_TransScale(g, 0.5, 1, 2, 3)) AS transscale_geom
FROM (
  SELECT ST_GeomFromText(input.wkt) AS g
  FROM (VALUES ($1), ($2)) AS input(wkt)
) AS source
