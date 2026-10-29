SELECT ST_AsEWKB(
         ST_AsMVTGeom(
           ST_GeomFromText($1::text),
           ST_GeomFromText($2::text)::box2d,
           $3::bigint::integer,
           $4::bigint::integer,
           $5::boolean
         )
       ) AS geom
