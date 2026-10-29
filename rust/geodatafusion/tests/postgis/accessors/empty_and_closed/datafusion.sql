SELECT ST_IsEmpty(ST_GeomFromText($1)) AS isempty,
       ST_IsClosed(ST_GeomFromText($1)) AS isclosed
