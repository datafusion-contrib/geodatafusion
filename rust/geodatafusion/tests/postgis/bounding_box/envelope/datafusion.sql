SELECT ST_AsEWKB(ST_Envelope(ST_GeomFromText($1))) AS geom
