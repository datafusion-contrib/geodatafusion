SELECT ST_AsBinary(ST_AddPoint(ST_GeomFromText($1), ST_GeomFromText($2), CAST($3 AS INT))) AS add_point,
       ST_AsBinary(ST_SetPoint(ST_GeomFromText($1), CAST($3 AS INT), ST_GeomFromText($5))) AS set_point,
       ST_AsBinary(ST_RemovePoint(ST_GeomFromText($1), CAST($3 AS INT))) AS remove_point,
       ST_AsBinary(ST_Reverse(ST_GeomFromText($4))) AS reverse,
       ST_AsBinary(ST_Scroll(ST_GeomFromText($1), ST_GeomFromText($2))) AS scroll
