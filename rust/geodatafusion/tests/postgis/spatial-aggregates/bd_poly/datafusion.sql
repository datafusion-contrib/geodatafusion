SELECT ST_AsEWKB(ST_BdPolyFromText($1, $2)) AS poly,
       ST_AsEWKB(ST_BdMPolyFromText($1, $2)) AS mpoly
