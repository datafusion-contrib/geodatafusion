SELECT ST_AsEWKT(ST_GeomFromEWKT($1)) AS ewkt,
       ST_AsHEXEWKB(ST_GeomFromEWKT($1)) AS hexewkb
