SELECT ST_AsEWKB(ST_GeometryN(ST_GeomFromText($1::text), ($2::int8)::int)) AS geometryn,
       ST_AsEWKB(ST_PointN(ST_GeomFromText($1::text), ($2::int8)::int)) AS pointn,
       ST_AsEWKB(ST_InteriorRingN(ST_GeomFromText($1::text), ($2::int8)::int)) AS interiorringn
