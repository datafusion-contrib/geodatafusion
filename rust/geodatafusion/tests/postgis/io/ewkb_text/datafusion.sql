SELECT ST_AsEWKT(ST_GeomFromEWKB(decode($1, 'hex'))) AS ewkt,
       ST_AsHEXEWKB(ST_GeomFromEWKB(decode($1, 'hex'))) AS hexewkb
