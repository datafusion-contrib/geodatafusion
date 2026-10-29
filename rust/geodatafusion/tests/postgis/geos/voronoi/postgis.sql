SELECT ST_AsEWKB(ST_VoronoiLines(ST_GeomFromText($1), $2, ST_GeomFromText($3))) AS lines,
       ST_AsEWKB(ST_VoronoiPolygons(ST_GeomFromText($1), $2, ST_GeomFromText($3))) AS polygons
