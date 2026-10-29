SELECT GeometryType(ST_GeomFromText($1::text)) AS geometrytype,
       ST_GeometryType(ST_GeomFromText($1::text)) AS st_geometrytype,
       ST_CoordDim(ST_GeomFromText($1::text))::text AS coorddim,
       ST_NDims(ST_GeomFromText($1::text))::text AS ndims
