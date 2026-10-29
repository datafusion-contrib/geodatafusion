SELECT ST_GeomFromText(input.wkt) AS geom_from_text,
       ST_GeometryFromText(input.wkt) AS geometry_from_text,
       ST_WKTToSQL(input.wkt) AS wkt_to_sql,
       ST_GeomFromWKB(decode(input.wkb_hex, 'hex')) AS geom_from_wkb,
       ST_WKBToSQL(decode(input.wkb_hex, 'hex')) AS wkb_to_sql,
       ST_AsBinary(ST_GeomFromText(input.wkt)) AS as_binary
FROM (VALUES (1, $1, $2), (2, $3, $4)) AS input(ord, wkt, wkb_hex)
ORDER BY input.ord
