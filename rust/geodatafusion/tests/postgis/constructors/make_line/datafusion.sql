SELECT ST_MakeLine(g1, g2) AS line_two,
       ST_MakeLine(g2, g1) AS line_reversed,
       ST_LineFromMultiPoint(mp) AS line_multipoint,
       ST_MakeLine(ST_PointFromText($1, 4326), ST_PointFromText($2, 4326)) AS line_typed_srid
FROM (
    SELECT ST_GeomFromText($1) AS g1,
           ST_GeomFromText($2) AS g2,
           ST_GeomFromText($3) AS mp
) AS source
