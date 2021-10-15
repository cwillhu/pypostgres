import re, tempfile
from os import path as p
from pypostgres.pypostgres import psql_command
from pypostgres.table import define_varchar_table_from_csv


def test_03__define_varchar_table_from_csv():

    tableName = 'test__define_varchar_table_from_csv'
    with tempfile.TemporaryDirectory() as tmpDir:

        tmpfile = p.join(tmpDir, f'temp__{tableName}.csv')
        with open(tmpfile, 'w') as t:
            t.write("TestCol1,TestCol2,TestCol3\n")
            t.write('1,,"foo"\n')
            t.write('2,10.5,"bar"\n')

        define_varchar_table_from_csv(tmpfile, tableName, schemaName='test')

    command=f'\\d+ test.{tableName};'
    res = psql_command(command)

    assert re.search(r'testcol1 \| character varying \|', res.text)        
    assert re.search(r'testcol2 \| character varying \|', res.text)        
    assert re.search(r'testcol3 \| character varying \|', res.text)        
    
    assert res.returnVal == 0 

    psql_command('drop table test.{tableName};')
    
