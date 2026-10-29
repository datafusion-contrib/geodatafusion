SELECT ST_AsBinary(ST_GeometryN(ST_GeomFromText($1), $2)) AS geometryn,
       ST_AsBinary(ST_PointN(ST_GeomFromText($1), $2)) AS pointn,
       ST_AsBinary(ST_InteriorRingN(ST_GeomFromText($1), $2)) AS interiorringn
