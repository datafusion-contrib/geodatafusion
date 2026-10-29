SELECT ST_IsEmpty(ST_GeomFromText($1::text)) AS isempty,
       ST_IsClosed(ST_GeomFromText($1::text)) AS isclosed
