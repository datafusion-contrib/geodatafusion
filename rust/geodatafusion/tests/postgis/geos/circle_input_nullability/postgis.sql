SELECT ST_MaximumInscribedCircle(ST_GeomFromText($1)) IS NULL AS mic_null,
       ST_LargestEmptyCircle(ST_GeomFromText($1)) IS NULL AS lec_null,
       ST_MinimumBoundingCircle(ST_GeomFromText($1)) IS NULL AS mbc_null,
       ST_MinimumBoundingRadius(ST_GeomFromText($1)) IS NULL AS mbr_null
