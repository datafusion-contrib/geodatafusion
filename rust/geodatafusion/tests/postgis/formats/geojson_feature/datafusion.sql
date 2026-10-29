WITH t AS (
    SELECT
        CAST(1 AS BIGINT) AS id,
        CAST('one' AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS nullable,
        ARRAY[1, CAST(NULL AS INT), 3] AS tags,
        named_struct(
            'f1', CAST(2 AS BIGINT),
            'f2', CAST('nested' AS VARCHAR)
        ) AS details,
        ST_GeomFromGeoJSON($1) AS geom
)
SELECT
    ST_AsGeoJSON(
        named_struct(
            'id', id,
            'name', name,
            'nullable', nullable,
            'tags', tags,
            'details', details,
            'geom', geom
        ),
        'geom', 9, false, 'id'
    ) AS feature,
    ST_AsGeoJSON(
        named_struct(
            'id', id,
            'name', name,
            'nullable', nullable,
            'tags', tags,
            'details', details,
            'geom', geom
        )
    ) AS default_feature,
    ST_AsGeoJSON(
        named_struct(
            'id', id,
            'name', name,
            'nullable', nullable,
            'tags', tags,
            'details', details,
            'geom', geom
        ),
        '', 3, true, 'id'
    ) AS pretty_feature,
    ST_AsGeoJSON(
        named_struct(
            'id', id,
            'name', name,
            'nullable', nullable,
            'tags', tags,
            'details', details,
            'geom', geom
        ),
        'geom', 9, false, 'nullable'
    ) AS nullable_id_feature
FROM t
