WITH candidate AS (
    SELECT ST_ConcaveHull(ST_GeomFromText($1), $2, $3) AS geom
)
SELECT
    ST_IsEmpty(geom) AS is_empty,
    ST_Area(geom) AS area,
    CAST(ST_NRings(geom) AS integer) AS nrings,
    CAST(ST_NDims(geom) AS integer) AS ndims,
    ST_GeometryType(geom) = $4 AS type_matches,
    ST_Equals(geom, ST_GeomFromText($5)) AS equals_expected
FROM candidate
