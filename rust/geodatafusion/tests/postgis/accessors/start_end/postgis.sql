SELECT ST_AsEWKB(ST_StartPoint(ST_GeomFromText($1::text))) AS startpoint,
       ST_AsEWKB(ST_EndPoint(ST_GeomFromText($1::text))) AS endpoint
