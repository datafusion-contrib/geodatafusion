SELECT CAST(ST_NPoints(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS DOUBLE) AS npoints,
       ST_Area(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS area,
       ST_Perimeter(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS perimeter,
       ST_XMin(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS xmin,
       ST_YMin(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS ymin,
       ST_XMax(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS xmax,
       ST_YMax(ST_MinimumBoundingCircle(ST_GeomFromText($1), $2)) AS ymax
