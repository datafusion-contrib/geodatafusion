SELECT GeometryType(ST_GeomFromText($1)) AS geometrytype,
       ST_GeometryType(ST_GeomFromText($1)) AS st_geometrytype,
       CAST(ST_CoordDim(ST_GeomFromText($1)) AS VARCHAR) AS coorddim,
       CAST(ST_NDims(ST_GeomFromText($1)) AS VARCHAR) AS ndims
