SELECT ST_PointFromText(arg1, 4326) AS point,
       ST_M(ST_PointFromText(arg1, 4326)) IS NULL AS m_is_null,
       ST_Z(ST_PointFromText(arg1, 4326)) IS NULL AS z_is_null,
       ST_Translate(ST_PointFromText(arg1, 4326), arg2, 0) IS NULL AS translated_is_null
FROM oracle_input
