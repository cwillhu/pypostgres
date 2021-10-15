from decimal import Decimal
from pypostgres.table import cast_to_new_table
from pypostgres.pypostgres import dbexecute, Connection

# test pypostgres.cast_to_new_table
def test_02__cast_to_new_table():

    tableName = 'test__cast_to_new_table'
    newTableName = f'{tableName}__temp'
    
    dbexecute('create schema if not exists test;')    

    #create test table with data to be casted

    dbexecute(f'drop table if exists test.{tableName};')

    dbexecute(f'create table test.{tableName} (anint integer, anumber numeric);')

    dbexecute(f'insert into test.{tableName} (anint, anumber) ' \
              + 'values (10, 10.5), (2, 100.1234), (0, -.1);')

    #put column specs in 'cols' var
    colNames = ['anint', 'anumber']
    oldColTypes = ['integer', 'numeric']
    newColTypes = ['numeric', 'varchar']
    cols = list(zip(colNames, oldColTypes, newColTypes))

    #perform cast to new table
    cast_to_new_table(tableName, newTableName, schemaName='test', cols=cols)

    #retrieve column names and types in new table
    with Connection() as c:
        c.cursor.execute('select column_name, data_type from information_schema.columns ' \
                         + f"where table_schema = 'test' and table_name = '{newTableName}';")
        res = c.cursor.fetchall()

    #check that new colums are of expected type
    assert res == [('anint', 'numeric'), ('anumber', 'character varying')]

    #retrieve casted data in new table
    with Connection() as c:
        c.cursor.execute(f'select * from test.{newTableName}')
        res = c.cursor.fetchall()

    #check that new data is correctly casted
    assert res == [(Decimal('10'), '10.5'), (Decimal('2'), '100.1234'), (Decimal('0'), '-0.1')]

    dbexecute(f'drop table if exists test.{tableName};')
    dbexecute(f'drop table if exists test.{newTableName};')    

    
if __name__ == '__main__':
    test_02__cast_to_new_table()
    
