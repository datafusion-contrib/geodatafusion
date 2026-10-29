SELECT ST_AsBinary(ST_Force2D(ST_GeomFromText($1))) AS force2d,
       ST_AsBinary(ST_FlipCoordinates(ST_GeomFromText($1))) AS flip_coordinates,
       ST_AsBinary(ST_Force3D(ST_GeomFromText($1), $2)) AS force3d,
       ST_AsBinary(ST_Force3DZ(ST_GeomFromText($1), $2)) AS force3dz,
       ST_AsBinary(ST_Force3DM(ST_GeomFromText($1), $3)) AS force3dm,
       ST_AsBinary(ST_Force4D(ST_GeomFromText($1), $2, $3)) AS force4d,
       ST_AsBinary(ST_SwapOrdinates(ST_GeomFromText($1), $4)) AS swap_ordinates
