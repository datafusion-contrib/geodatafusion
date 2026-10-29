# Largest empty circle tolerance contract

The asymmetric four-point case is an approximate optimum whose maximizer is
not unique. The exact optimum lies where the convex-hull edge from `(10 0)` to
`(0 6)` meets the perpendicular bisector of `(10 0)` and `(3 1)`:

```
3x + 5y = 30
7x - y  = 45
```

That gives center `(255/38, 75/38)` and radius `sqrt(21250)/38`, approximately
`3.8361525623982238`. GEOS 3.12 changed the initial search grid, so GEOS
3.11.5 and GEOS 3.14.1 return different feasible centers and witness points;
both radii are within the caller's `0.01` tolerance of the exact optimum.

This group checks the observable contract instead of a bitwise snapshot:

- the center is not outside the convex-hull boundary;
- the witness is one of the input obstacles;
- `radius` equals the center-to-witness distance;
- the circle does not contain an obstacle;
- the radius is within tolerance below the exact optimum and never above it.
