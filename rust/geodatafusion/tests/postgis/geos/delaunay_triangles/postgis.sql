SELECT ST_AsEWKB(ST_DelaunayTriangles(ST_GeomFromText($1), $2, 0)) AS polygons,
       ST_AsEWKB(ST_DelaunayTriangles(ST_GeomFromText($1), $2, 1)) AS edges
