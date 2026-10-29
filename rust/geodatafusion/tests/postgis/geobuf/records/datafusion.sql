SELECT ST_AsGeobuf(named_struct(
  'geom', ST_GeomFromText(arg1),
  'name', arg2,
  'n', arg3,
  'nullable', arg4
)) AS geobuf
FROM oracle_input
