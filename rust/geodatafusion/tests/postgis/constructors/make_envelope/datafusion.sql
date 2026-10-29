SELECT ST_MakeEnvelope($1, $2, $3, $4) AS envelope_default,
       ST_MakeEnvelope($1, $2, $3, $4, $5) AS envelope_srid
