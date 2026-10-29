SELECT ST_PointFromWKB(decode($1, 'hex')) AS point,
       ST_PointFromWKB(decode($1, 'hex'), $2) AS point_srid,
       ST_LineFromWKB(decode($1, 'hex')) AS line,
       ST_LineFromWKB(decode($1, 'hex'), $2) AS line_srid,
       ST_LinestringFromWKB(decode($1, 'hex')) AS linestring_alias,
       ST_LinestringFromWKB(decode($1, 'hex'), $2) AS linestring_alias_srid
