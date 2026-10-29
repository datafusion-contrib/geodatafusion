SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON($1)) AS default_json,
       ST_AsGeoJSON(ST_GeomFromGeoJSON($1), 9, 1) AS bbox_json,
       ST_AsGeoJSON(ST_GeomFromGeoJSON($1), 9, 2) AS short_crs_json,
       ST_AsGeoJSON(ST_GeomFromGeoJSON($1), 9, 4) AS long_crs_json,
       ST_AsGeoJSON(ST_GeomFromGeoJSON($1), 3, 0) AS precision_json
