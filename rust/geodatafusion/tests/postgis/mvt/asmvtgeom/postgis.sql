SELECT ST_AsEWKB(
         ST_AsMVTGeom(
           ST_GeomFromText($1::text),
           ST_MakeBox2D(ST_Point($2::float8, $3::float8), ST_Point($4::float8, $5::float8)),
           $6::bigint::integer,
           $7::bigint::integer,
           $8::boolean
         )
       ) AS geom
