SELECT ST_AsEWKB(ST_BdPolyFromText($1, CAST(CAST($2 AS bigint) AS integer))) AS poly,
       ST_AsEWKB(ST_BdMPolyFromText($1, CAST(CAST($2 AS bigint) AS integer))) AS mpoly
