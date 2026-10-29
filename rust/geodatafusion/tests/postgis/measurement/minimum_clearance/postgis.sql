SELECT ST_MinimumClearance(ST_GeomFromText($1)) AS clearance,
       ST_AsEWKB(ST_MinimumClearanceLine(ST_GeomFromText($1))) AS clearance_line
