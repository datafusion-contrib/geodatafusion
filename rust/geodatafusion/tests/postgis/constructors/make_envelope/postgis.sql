SELECT ST_AsEWKB(ST_MakeEnvelope($1, $2, $3, $4)) AS envelope_default,
       ST_AsEWKB(ST_MakeEnvelope($1, $2, $3, $4, CAST(CAST($5 AS bigint) AS integer))) AS envelope_srid
