SELECT ST_AsEWKB(ST_GeomFromTWKB(decode($1, 'hex'))) AS geom
