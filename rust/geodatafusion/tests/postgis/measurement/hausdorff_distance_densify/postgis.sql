SELECT ST_HausdorffDistance(ST_GeomFromText($1), ST_GeomFromText($2), $3) AS value
