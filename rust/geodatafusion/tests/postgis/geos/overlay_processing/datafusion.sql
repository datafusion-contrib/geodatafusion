SELECT ST_Difference(g1, g2) AS difference,
       ST_Intersection(g1, g2) AS intersection,
       ST_SymDifference(g1, g2) AS symdifference,
       ST_UnaryUnion(g1) AS unaryunion,
       ST_Node(g1) AS node,
       ST_Buffer(g1, 0) AS buffer,
       ST_BuildArea(g1) AS buildarea
FROM (
    SELECT ST_GeomFromText($1) AS g1, ST_GeomFromText($2) AS g2
) AS inputs
