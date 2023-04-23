import pyodbc
import gspread
import sqlalchemy
from os import path
from LOGICA.GENERAL.constantes import N_SERVIDOR,N_BBDD
from LOGICA.GENERAL.constantes import P_ACCESOS
from LOGICA.GENERAL.constantes import FN_CREDENTIALS,FN_AUTH_USER


# Obtencion del driver de SQL
def get_mssql_driver() : #-> str
    mssql_drivers = ['SQL Server Native Client 11.0', 'SQL Server']
    mssql_drivers = [d for d in mssql_drivers if d in pyodbc.drivers()]

    if len(mssql_drivers) == 0:
        raise Exception('ERROR: El sistema no cuenta con drivers para SQL Server compatibles')
    
    return mssql_drivers[0]


# Conexion a SQL
def conecta_sql() : 
    driver = get_mssql_driver() # Obteniendo el driver
    try:
        #cadena_conexion = 'DRIVER={};SERVER={};TRUSTED_CONNECTION=YES;'.format(driver, N_SERVIDOR)
        #conexion =  pyodbc.connect(cadena_conexion, autocommit=True)
        motor_sql = sqlalchemy.create_engine('mssql+pyodbc://{}/{}?trusted_connection=yes&driver={}'.format(N_SERVIDOR, N_BBDD, driver),
                                        connect_args={'connect_timeout': 10}, echo=False)  
    except Exception as e:
        print('Error en conexion a BBDD:', e)
    
    return motor_sql

# Conexion a Google Sheet

def conecta_gs():
    fp_credentials =  path.join(P_ACCESOS,FN_CREDENTIALS)
    fp_auth_user =  path.join(P_ACCESOS,FN_AUTH_USER)

    conexion_gs = gspread.oauth(
        credentials_filename= fp_credentials#,
        #authorized_user_filename= fp_auth_user
    )

    return conexion_gs

