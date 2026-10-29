SELECT d.valid AS valid, d.reason AS reason, ST_AsEWKB(d.location) AS location
FROM (SELECT ST_IsValidDetail(ST_GeomFromText($1), $2) AS d) AS s
