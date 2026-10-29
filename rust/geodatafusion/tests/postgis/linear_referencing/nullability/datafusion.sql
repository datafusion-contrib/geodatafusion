SELECT ST_LineInterpolatePoint(ST_GeomFromText(arg1), arg2) IS NULL AS interpolate_point_null,
       ST_3DLineInterpolatePoint(ST_GeomFromText(arg1), arg2) IS NULL AS interpolate_point_3d_null,
       ST_LineInterpolatePoints(ST_GeomFromText(arg1), arg2) IS NULL AS interpolate_points_null,
       ST_LineSubstring(ST_GeomFromText(arg1), arg2, arg3) IS NULL AS substring_null,
       ST_LocateAlong(ST_GeomFromText(arg1), arg2) IS NULL AS locate_along_null,
       ST_LocateBetween(ST_GeomFromText(arg1), arg2, arg3) IS NULL AS locate_between_null,
       ST_LocateBetweenElevations(ST_GeomFromText(arg1), arg2, arg3) IS NULL AS locate_between_elevations_null,
       ST_AddMeasure(ST_GeomFromText(arg1), arg2, arg3) IS NULL AS add_measure_null,
       ST_LineLocatePoint(ST_GeomFromText(arg1), ST_GeomFromText(arg4)) IS NULL AS line_locate_point_null,
       ST_InterpolatePoint(ST_GeomFromText(arg1), ST_GeomFromText(arg4)) IS NULL AS interpolate_point_m_null
FROM oracle_input
