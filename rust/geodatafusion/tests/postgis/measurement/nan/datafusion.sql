SELECT
    ST_Length(ST_GeomFromWKB(decode($1, 'hex'))) AS length,
    ST_3DLength(ST_GeomFromWKB(decode($2, 'hex'))) AS length3,
    ST_Perimeter(ST_GeomFromWKB(decode($3, 'hex'))) AS perimeter,
    ST_3DPerimeter(ST_GeomFromWKB(decode($4, 'hex'))) AS perimeter3,
    ST_Azimuth(
        ST_GeomFromWKB(decode($5, 'hex')),
        ST_GeomFromWKB(decode($6, 'hex'))
    ) AS azimuth,
    ST_Angle(
        ST_GeomFromWKB(decode($7, 'hex')),
        ST_GeomFromWKB(decode($8, 'hex')),
        ST_GeomFromWKB(decode($9, 'hex')),
        ST_GeomFromWKB(decode($10, 'hex'))
    ) AS angle
