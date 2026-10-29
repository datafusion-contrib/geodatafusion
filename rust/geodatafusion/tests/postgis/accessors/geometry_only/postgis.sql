SELECT ST_Dimension(ST_GeomFromText($1::text))::text AS dimension,
       ST_IsCollection(ST_GeomFromText($1::text)) AS iscollection,
       ST_NumGeometries(ST_GeomFromText($1::text))::text AS numgeometries,
       ST_NRings(ST_GeomFromText($1::text))::text AS nrings,
       ST_Zmflag(ST_GeomFromText($1::text))::text AS zmflag,
       ST_HasZ(ST_GeomFromText($1::text)) AS hasz,
       ST_HasM(ST_GeomFromText($1::text)) AS hasm
