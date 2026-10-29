SELECT ST_NPoints(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer)))::double precision AS npoints,
       ST_Area(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer))) AS area,
       ST_Perimeter(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer))) AS perimeter,
       ST_XMin(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer))) AS xmin,
       ST_YMin(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer))) AS ymin,
       ST_XMax(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer))) AS xmax,
       ST_YMax(ST_MinimumBoundingCircle(ST_GeomFromText($1), CAST(CAST($2 AS bigint) AS integer))) AS ymax
