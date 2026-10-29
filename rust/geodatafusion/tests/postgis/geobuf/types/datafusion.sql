SELECT ST_AsGeobuf(named_struct(
  'geom', ST_GeomFromText(arg1),
  'name', arg2,
  'n', arg3,
  'flag', arg4,
  'value', arg5,
  'nullable', arg6
), 'geom') AS geobuf
FROM oracle_input
