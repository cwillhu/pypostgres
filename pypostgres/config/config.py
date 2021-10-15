import yaml
from pathlib import Path
from urllib.parse import quote_plus as urlquote
from importlib_resources import files, as_file
import pypostgres.config

data_resource = files(pypostgres.config).joinpath('config.yaml')

class Config:

    # Read default config 
    with as_file(data_resource) as yamlfile:
        with open(yamlfile) as f:
            _conf = yaml.load(f, Loader=yaml.Loader)    
    
    _dbName = _conf['database']['databasename']
    _dbHost = _conf['database']['hostname']
    _dbUser = _conf['database']['username']
    _dbPW   = _conf['database']['password']
    _dbPort = str(_conf['database']['port'])
    
    #sqlalchemy database URL
    _postgresURL = 'postgresql+psycopg2://' + _dbUser +':'+ _dbPW +'@'+ _dbHost +':'+ _dbPort +'/'+ _dbName

    #psycopg2 connection string
    _dbPW_escape1 = _dbPW.replace("'", "\\'")

    #password for subprocess.Popen("psql ...") 
    _dbPW_escape2 = _dbPW.replace("'", "\\\'").replace("`","\\`")
    
    @staticmethod
    def get(name):

        if name == 'psycopg2_conn_string':  

            return(f"host={Config._dbHost} user={Config._dbUser} password='{Config._dbPW_escape1}' dbname={Config._dbName} sslmode=require")

        elif name == 'sqlalchemy_psycopg2_uri':

            return(f"postgresql+psycopg2://{Config._dbUser}:{urlquote(Config._dbPW)}@{Config._dbHost}/{Config._dbName}?sslmode=require")

        elif name == 'psql_client_string':  # Prefix string for psql command

            return(f'psql "sslmode=require host={Config._dbHost} dbname={Config._dbName} user={Config._dbUser} password=\'{Config._dbPW_escape2}\'" ')

        else:

            return(getattr(Config, '_' + name))

    @staticmethod
    def set(name, value):
        setattr(Config, '_' + name, value)

