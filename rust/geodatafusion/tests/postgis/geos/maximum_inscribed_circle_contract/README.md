# Maximum inscribed circle tolerance contract

`GEOSMaximumInscribedCircle` is an iterative optimizer whose result is only
specified to the requested distance tolerance. GEOS 3.14 added the exact
`ExactMaxInscribedCircle` path for simple convex polygons, so the same input
can return different bits on older and newer runtimes even though both outputs
are valid.

This group checks the observable contract instead of a bitwise snapshot:

- the center lies inside the input polygon;
- the returned witness lies on the polygon boundary;
- `radius` equals the center-to-witness distance;
- the center and radius stay within the tolerance passed by the caller.

The reference center and radius are the exact incenter and inradius:
for the square they are `(5, 5, 5)`; for the right triangle with legs of
length 10 they are `10 - 5 * sqrt(2)`.
