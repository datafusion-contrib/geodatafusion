SELECT ST_NPoints(ST_GeomFromText($1::text))::text AS npoints,
       ST_NumPoints(ST_GeomFromText($1::text))::text AS numpoints,
       ST_NumInteriorRings(ST_GeomFromText($1::text))::text AS numinteriorrings,
       ST_NumInteriorRing(ST_GeomFromText($1::text))::text AS numinteriorring
