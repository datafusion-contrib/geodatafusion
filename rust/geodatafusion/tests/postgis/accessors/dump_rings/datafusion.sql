SELECT component.path AS path, ST_AsBinary(component.geom) AS geom
FROM (SELECT UNNEST(ST_DumpRings(ST_GeomFromText($1))) AS component)
