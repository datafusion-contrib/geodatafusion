SELECT ST_AsEWKB(ST_Force2D(ST_GeomFromText($1::text))) AS force2d,
       ST_AsEWKB(ST_FlipCoordinates(ST_GeomFromText($1::text))) AS flip_coordinates,
       ST_AsEWKB(ST_Force3D(ST_GeomFromText($1::text), $2::float8)) AS force3d,
       ST_AsEWKB(ST_Force3DZ(ST_GeomFromText($1::text), $2::float8)) AS force3dz,
       ST_AsEWKB(ST_Force3DM(ST_GeomFromText($1::text), $3::float8)) AS force3dm,
       ST_AsEWKB(ST_Force4D(ST_GeomFromText($1::text), $2::float8, $3::float8)) AS force4d,
       ST_AsEWKB(ST_SwapOrdinates(ST_GeomFromText($1::text), CAST($4::text AS cstring))) AS swap_ordinates
