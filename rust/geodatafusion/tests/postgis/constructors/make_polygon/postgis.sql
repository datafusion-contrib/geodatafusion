SELECT ST_AsEWKB(ST_MakePolygon(shell)) AS polygon_shell,
       ST_AsEWKB(ST_Polygon(shell, CAST(CAST($4 AS bigint) AS integer))) AS polygon_srid
FROM (
    SELECT ST_GeomFromText($1) AS shell,
           ST_GeomFromText($2) AS hole1,
           ST_GeomFromText($3) AS hole2
) AS source
