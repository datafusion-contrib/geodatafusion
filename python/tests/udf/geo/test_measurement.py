from datafusion import SessionContext
from geodatafusion import register_all


def test_st_perimeter_geometry_collection():
    ctx = SessionContext()
    register_all(ctx)
    sql = """
        SELECT ST_Perimeter(ST_GeomFromText(
            'GEOMETRYCOLLECTION(POLYGON((0 0,4 0,4 3,0 3,0 0)),POLYGON((10 10,12 10,12 12,10 12,10 10)))'
        )) AS perimeter
    """
    table = ctx.sql(sql).to_arrow_table()
    assert table.column("perimeter")[0].as_py() == 22.0
