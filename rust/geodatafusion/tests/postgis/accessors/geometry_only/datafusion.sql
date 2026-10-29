SELECT CAST(ST_Dimension(ST_GeomFromText($1)) AS VARCHAR) AS dimension,
       ST_IsCollection(ST_GeomFromText($1)) AS iscollection,
       CAST(ST_NumGeometries(ST_GeomFromText($1)) AS VARCHAR) AS numgeometries,
       CAST(ST_NRings(ST_GeomFromText($1)) AS VARCHAR) AS nrings,
       CAST(ST_Zmflag(ST_GeomFromText($1)) AS VARCHAR) AS zmflag,
       ST_HasZ(ST_GeomFromText($1)) AS hasz,
       ST_HasM(ST_GeomFromText($1)) AS hasm
