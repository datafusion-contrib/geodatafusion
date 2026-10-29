SELECT component.path AS path, ST_AsBinary(component.geom) AS geom
FROM (SELECT UNNEST(ST_DumpSegments(ST_GeomFromText($1))) AS component)
