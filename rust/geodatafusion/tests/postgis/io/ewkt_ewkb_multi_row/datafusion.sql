SELECT ST_AsEWKT(ST_GeomFromEWKT(input.ewkt)) AS ewkt,
       ST_AsHEXEWKB(ST_GeomFromEWKB(decode(input.ewkb, 'hex'))) AS hexewkb
FROM (VALUES ($1, $2), ($3, $4), ($5, $6)) AS input(ewkt, ewkb)
