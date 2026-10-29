SELECT CAST(ST_NPoints(ST_GeomFromText($1)) AS VARCHAR) AS npoints,
       CAST(ST_NumPoints(ST_GeomFromText($1)) AS VARCHAR) AS numpoints,
       CAST(ST_NumInteriorRings(ST_GeomFromText($1)) AS VARCHAR) AS numinteriorrings,
       CAST(ST_NumInteriorRing(ST_GeomFromText($1)) AS VARCHAR) AS numinteriorring
