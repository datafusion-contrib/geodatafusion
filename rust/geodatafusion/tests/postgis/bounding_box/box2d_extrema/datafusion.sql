SELECT ST_XMin(Box2D(ST_GeomFromText($1))) AS xmin, ST_YMin(Box2D(ST_GeomFromText($1))) AS ymin, ST_XMax(Box2D(ST_GeomFromText($1))) AS xmax, ST_YMax(Box2D(ST_GeomFromText($1))) AS ymax
