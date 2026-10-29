SELECT ST_3DShortestLine(ST_GeomFromText($1), ST_GeomFromText($2)) AS geom
