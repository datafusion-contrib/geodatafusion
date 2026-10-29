SELECT ST_AsEWKB(ST_PointFromWKB(decode($1, 'hex'))) AS point,
       ST_AsEWKB(ST_PointFromWKB(decode($1, 'hex'), CAST(CAST($2 AS bigint) AS integer))) AS point_srid,
       ST_AsEWKB(ST_LineFromWKB(decode($1, 'hex'))) AS line,
       ST_AsEWKB(ST_LineFromWKB(decode($1, 'hex'), CAST(CAST($2 AS bigint) AS integer))) AS line_srid,
       ST_AsEWKB(ST_LinestringFromWKB(decode($1, 'hex'))) AS linestring_alias,
       ST_AsEWKB(ST_LinestringFromWKB(decode($1, 'hex'), CAST(CAST($2 AS bigint) AS integer))) AS linestring_alias_srid
