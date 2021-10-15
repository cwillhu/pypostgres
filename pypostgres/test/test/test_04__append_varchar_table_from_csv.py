from pypostgres.pypostgres import Connection, dbexecute
from pypostgres.table import define_varchar_table_from_csv, append_varchar_table_from_csv
import re, tempfile
from os import path as p
import pandas as pd
import numpy as np


def test_04__append_varchar_table_from_csv():

    with tempfile.TemporaryDirectory() as tmpDir:

        tmpfile = p.join(tmpDir, 'temp__define_varchar_table_from_csv.csv')
        with open(tmpfile, 'w') as t:
            t.write("TestCol1,TestCol2,TestCol3\n")
            t.write('1,"","foo"\n')  #in EPA files, missing numeric values are represented as ""
            t.write('2,10.5,"bar"\n')

        tableName = 'test__append_varchar_table_from_csv'

        define_varchar_table_from_csv(tmpfile, tableName=tableName, schemaName='test')
        append_varchar_table_from_csv(tmpfile, tableName=tableName, schemaName='test')

        # Read table just loaded from csvfile
        with Connection() as c:
            df1 = pd.read_sql(f"select * from test.{tableName};", c.conn)

        testcol1 = ["1", "2"]
        testcol2 = ["", "10.5"]
        testcol3 = ["foo", "bar"]
        df1_expected = pd.DataFrame({'testcol1': testcol1, 'testcol2': testcol2, 'testcol3': testcol3})

        assert df1.equals(df1_expected)        

        # test that a second function call appends and does not overwrite
        append_varchar_table_from_csv(tmpfile, tableName=tableName, schemaName='test')

    # read table just modidfied
    with Connection() as c:
        df2 = pd.read_sql(f"select * from test.{tableName};", c.conn)
        
    testcol1.extend(testcol1)
    testcol2.extend(testcol2)
    testcol3.extend(testcol3)
    df2_expected = pd.DataFrame({'testcol1': testcol1, 'testcol2': testcol2, 'testcol3': testcol3})        
         
    assert df2.equals(df2_expected)

    dbexecute(f'drop table test.{tableName};')        
