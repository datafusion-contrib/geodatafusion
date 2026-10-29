SELECT ST_AsEWKB(ST_OrientedEnvelope(ST_GeomFromText($1))) AS geom
