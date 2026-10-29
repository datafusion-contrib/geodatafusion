SELECT ST_AsEncodedPolyline(ST_GeomFromGeoJSON($1), $2) AS encoded
