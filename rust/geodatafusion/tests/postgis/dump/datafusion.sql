SELECT component.path AS path, component.geom AS geom
FROM (
  SELECT UNNEST(ST_Dump(ST_GeomFromText($1))) AS component
)
