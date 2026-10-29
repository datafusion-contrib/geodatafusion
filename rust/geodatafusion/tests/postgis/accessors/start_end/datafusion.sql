SELECT ST_AsBinary(ST_StartPoint(ST_GeomFromText($1))) AS startpoint,
       ST_AsBinary(ST_EndPoint(ST_GeomFromText($1))) AS endpoint
