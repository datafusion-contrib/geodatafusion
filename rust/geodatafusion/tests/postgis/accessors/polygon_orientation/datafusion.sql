SELECT ST_IsPolygonCCW(ST_GeomFromText($1)) AS ccw,
       ST_IsPolygonCW(ST_GeomFromText($1)) AS cw
