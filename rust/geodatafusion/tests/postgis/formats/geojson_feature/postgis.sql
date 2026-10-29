WITH t AS (
    SELECT
        1::bigint AS id,
        'one'::text AS name,
        NULL::text AS nullable,
        ARRAY[1, NULL, 3]::int[] AS tags,
        (
            SELECT ROW(d.*)
            FROM (
                SELECT
                    2::bigint AS f1,
                    'nested'::text AS f2
            ) d
        ) AS details,
        ST_GeomFromGeoJSON($1) AS geom
)
SELECT
    ST_AsGeoJSON(t.*, 'geom', 9, false, 'id') AS feature,
    ST_AsGeoJSON(t.*) AS default_feature,
    ST_AsGeoJSON(t.*, ''::text, 3, true, 'id') AS pretty_feature,
    ST_AsGeoJSON(t.*, 'geom', 9, false, 'nullable') AS nullable_id_feature
FROM t
