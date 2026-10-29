SELECT r.id::text AS id, r.name AS name, ST_AsEWKB(r.geom) AS geom
FROM ST_FromFlatGeobuf(
    ROW(NULL::int, NULL::geometry, NULL::int, NULL::text),
    decode($1, 'hex')
) AS r(fid int, geom geometry, id int, name text)
ORDER BY r.fid
