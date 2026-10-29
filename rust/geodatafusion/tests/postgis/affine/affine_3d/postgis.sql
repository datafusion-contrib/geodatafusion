SELECT ST_AsEWKB(ST_Affine(
  ST_GeomFromText($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
)) AS geom
