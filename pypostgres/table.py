import sys
from os import path as p
from pypostgres.util import SubbableStr, get_formattedNames
from pypostgres.pypostgres import dbexecute, psql_command
from pypostgres.config.config import Config
from pypostgres.util import subproc


def set_geometry_from_latlon(tableName, schemaName, logger=None):
    ''' Set geometry col using columns latitude and longitude '''

    if logger: logger.info(f'Create geom column for {schemaName}.{tableName}')

    dbexecute(f"create table {schemaName}_{tableName}_temp as " \
              + "select *, geom = st_setsrid(st_makepoint(longitude, latitude), 4326) " \
              + "from {schemaName}.{tableName};")

    dbexecute(f"drop table {schemaName}.{tableName};")
    dbexecute(f"alter table {schemaName}.{tableName}_temp rename to {schemaName}.{tableName};")

    if logger: logger.info(f'Create geom column for {schemaName}.{tableName}: DONE')
    
    
def create_spatial_index(tableName, schemaName, indexMethod='gist', logger=None):
    ''' Create spatial index using 'geom' column '''

    if logger: logger.info(f"Create spatial index on 'geom' using mehod '{indexMethod}' for table {schemaName}.{tableName}")
    
    indexName = f'{schemaName}_{tableName}_{indexMethod}_idx'  # note: indexes are always created in same schema as parent table
    dbexecute(f'drop index if exists {schemaName}.{indexName};')
    dbexecute(f'create index {indexName} on {schemaName}.{tableName} using {indexMethod} (geom);')
    
    if logger: logger.info(f"Create spatial index on 'geom' using mehod '{indexMethod}' for table {schemaName}.{tableName}: DONE")

    
def create_btree_index(colName, tableName, schemaName, lower=False, logger=None):
    ''' Create btree index on column '''

    if logger: logger.info(f"Create btree index on {colName} in table {schemaName}.{tableName}. lower={lower}")

    if lower:
        indexName = f'{schemaName}_{tableName}_{colName}_lower_btree_idx'
    else:
        indexName = f'{schemaName}_{tableName}_{colName}_btree_idx'           

    dbexecute(f'drop index if exists {schemaName}.{indexName};')

    if lower:
        dbexecute(f'create index {indexName} on {schemaName}.{tableName} ((lower({colName})));')
    else:
        dbexecute(f'create index {indexName} on {schemaName}.{tableName} ({colName});')
        
    if logger: logger.info(f"Create btree index on {colName} in table {schemaName}.{tableName}. lower={lower}: DONE")
    

def append_csv(tableName, schemaName, csvfile, header=True, logger=None):
    ''' Append data in csvfile to table '''

    if logger: logger.info(f'Table {schemaName}.{tableName}: loading file {csvfile}')

    psql_client = Config.get('psql_client_string')
        
    if header:
        tailpipe = ' tail +2 | '
    else:
        tailpipe = ''
        
    # Load csvfile. Replace missing values ("") with null.
    copy_command = f"cat {csvfile} | {tailpipe} sed --regexp-extended -e 's/\\\"\\\"/null/g' | " + psql_client \
                  + rf''' --no-psqlrc --echo-all -c "\copy {schemaName}.{tableName} from stdin with (format csv, null 'null')" '''

    res = subproc(copy_command)
    if res.returnVal != 0:
        if logger: logger.error(res.text, stack_info=True)
        print(res.text)
        sys.exit(1)

    if logger: logger.info(f'Table {schemaName}.{tableName}: loading file {csvfile}: DONE')

                           
def create_table(tableName, schemaName, cols, logger=None):
    ''' Create table from list of tuples (colName, colType) '''

    if logger: logger.info(f'Creating table {schemaName}.{tableName}')

    # Column definitions string
    colDefs = [f'{x[0]} {x[1]}' for x in cols]
    colDefs_str = ', '.join(colDefs)

    # Drop and create table
    dbexecute(f'drop table if exists {schemaName}.{tableName};')
    dbexecute(f'create table {schemaName}.{tableName} ( {colDefs_str} );')


# Create a table and set column names according to a csvfile header
def define_varchar_table_from_csv(csvfile, tableName, schemaName=None, logger=None):

    if not schemaName:
        schemaName = 'public'
        
    # Build column definition string from csv headerline
    colNames = get_formattedNames(csvfile)
    colDefs = [f'{name} varchar' for name in colNames]
    colDefs_str = ', '.join(colDefs)

    # Drop and create table
    dbexecute(f'create schema if not exists {schemaName};')    
    dbexecute(f'drop table if exists {schemaName}.{tableName};')    

    create_cmd = f'create table {schemaName}.{tableName} ( {colDefs_str} );'
    if logger: logger.info(f'Creating table with: {create_cmd}')
    dbexecute(create_cmd)


# Append data in csvfile to table
def append_varchar_table_from_csv(csvfile, tableName, schemaName, skipHeader=True, logger=None):

    if skipHeader:
        copy_command = f"\\copy {schemaName}.{tableName} from program 'tail -n +2 {csvfile}' with (format csv)"
    else:
        copy_command = f"\\copy {schemaName}.{tableName} from program 'cat {csvfile}' with (format csv)"        

    if logger: logger.info(f'  Loading: {copy_command}')
    psql_command(copy_command)


# Cast columns to a new table.
# cols parameter is a list of tuples where each tuple contains three strings: column name, old type, new type.
# Only columns that appear in cols are included in the new table.
# To avoid casting a column, set old type and/or new type to None, or set both to the same value.
def cast_to_new_table(tableName, newTableName, schemaName, cols):

    selectStrings = []

    for col in cols:
        colName = col[0]
        oldType = col[1]
        newType = col[2]

        # build selected column strings for sql command, preserving column order

        if oldType == newType or oldType is None or newType is None:

            # no cast
            selectStrings.append(colName)

        else:

            # cast
            if oldType == 'varchar':
                selectStrings.append(f"cast(nullif({colName}, '') as {newType})")
            else:
                selectStrings.append(f'cast({colName} as {newType})')

    cast_cols_string = ', '.join(selectStrings)
    colNames_string = ', '.join([col[0] for col in cols])
    
    ##
    # Execute select/cast
    ##

    # drop and create table to receive casted columns
    dbexecute(f'drop table if exists {schemaName}.{newTableName};')

    newColumnDefs = ', '.join([f'{col[0]} {col[2]}' for col in cols])
    cmd = f'create table {schemaName}.{newTableName} ({newColumnDefs});'
    dbexecute(cmd)
    
    # Perform cast
    cmd = f'insert into {schemaName}.{newTableName} ({colNames_string}) ' \
          + f'select {cast_cols_string} from {schemaName}.{tableName};'
    dbexecute(cmd)    

    
##
# Notes
##

# '\copy' example:
#psql --no-psqlrc -c "\copy epa.eight_hour from '/mnt/scratch/epa/extract/8hour_42101/8hour_42101_1997.csv' delimiter ',' csv header"

# note: psql 'copy' requires superuser privs:
#dbexecute(f"COPY epa.{tableName} ({colNames_str}) " +
#          f"FROM '/mnt/scratch/epa/extract/8hour_42101/8hour_42101_1997.csv' DELIMITER ',' CSV HEADER;")

# In postgres 12+: geom can be a generated col:
#dbexecute(f'create table "epa.{tableName}" ( ' + \
#          f'   {colDefs_str}, ' + \
#          f'   geom geometry(Point,4326) generated always as (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) stored );')



