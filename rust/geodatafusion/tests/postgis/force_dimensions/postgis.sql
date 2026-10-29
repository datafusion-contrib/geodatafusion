SELECT
  ST_AsEWKB(ST_Force2D(ST_GeomFromText($1))) AS force_2d,
  ST_AsEWKB(ST_Force3D(ST_GeomFromText($1))) AS force_3d_default,
  ST_AsEWKB(ST_Force3D(ST_GeomFromText($1), $2)) AS force_3d_custom,
  ST_AsEWKB(ST_Force3DZ(ST_GeomFromText($1))) AS force_3dz_default,
  ST_AsEWKB(ST_Force3DZ(ST_GeomFromText($1), $2)) AS force_3dz_custom,
  ST_AsEWKB(ST_Force3DM(ST_GeomFromText($1))) AS force_3dm_default,
  ST_AsEWKB(ST_Force3DM(ST_GeomFromText($1), $3)) AS force_3dm_custom,
  ST_AsEWKB(ST_Force4D(ST_GeomFromText($1))) AS force_4d_default,
  ST_AsEWKB(ST_Force4D(ST_GeomFromText($1), $2, $3)) AS force_4d_custom
