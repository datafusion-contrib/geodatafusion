SELECT ST_MaximumInscribedCircle(ST_GeomFromText(arg1)) IS NULL AS mic_null,
       ST_LargestEmptyCircle(ST_GeomFromText(arg1)) IS NULL AS lec_null,
       ST_MinimumBoundingCircle(ST_GeomFromText(arg1)) IS NULL AS mbc_null,
       ST_MinimumBoundingRadius(ST_GeomFromText(arg1)) IS NULL AS mbr_null
FROM oracle_input
