SELECT ST_AsBinary(ST_ExteriorRing(ST_GeomFromText($1))) AS exteriorring
