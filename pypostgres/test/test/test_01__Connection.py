from pypostgres.pypostgres import Connection


# test db connection

def test_01__Connection():

    #read number of cols in a table
    with Connection() as c:
        c.cursor.execute("select count(*) from information_schema.columns where table_name='pg_stats';")
        ncols = c.cursor.fetchall()[0][0]
    
    assert ncols == 14  #pg_stats table has 14 cols
