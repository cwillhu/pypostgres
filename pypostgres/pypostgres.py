import psycopg2, csv
from os import path as p
from pypostgres.config.config import Config
from pypostgres.util import SubbableStr, subproc


# Connect to db, with clean up
class Connection():

    def __enter__(self):
        self.conn_string = Config.get('psycopg2_conn_string')
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

        
# Execute a psql command on db server via psycopg2
def dbexecute(command):

    with Connection() as c:
        c.cursor.execute(command)
        c.conn.commit()

        
# Have psql client execute a psql command
def psql_command(command, autocommit='on', on_error_stop='on'):

    if autocommit not in ['on', 'off']:
        raise ValueError(f'Invalid autocommit value: {autocommit}')
    if on_error_stop not in ['on', 'off']:
        raise ValueError(f'Invalid on_error_stop value: {on_error_stop}')    

    psql_client = Config.get('psql_client_string')
    
    subproc_command = psql_client + f'--set AUTOCOMMIT={autocommit} ' \
                                     + f'--set ON_ERROR_STOP={on_error_stop} ' \
                                     + f'--no-psqlrc --echo-all -c "{command}"'
    return subproc(subproc_command)

