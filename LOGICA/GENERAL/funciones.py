import sqlalchemy
from pandas import read_sql_query
from LOGICA.GENERAL.conexion import conecta_sql, get_mssql_driver

def ejecuta_query(query, params = None):
    
    conexion = conecta_sql() # Llamando a la conexión

    cursor = conexion.cursor() # Creando cursor
    try:
        if params is None: # Ejecutando query
            cursor.execute(query) 
        else:
            cursor.execute(query, params) 
    except Exception as e:
        print('Error en ejecucion del query')
        print(e)
        quit()

    cursor.close() # Cerrando cursor    
    del cursor # Borrando cursor
    conexion.close() # Cerrando conexion


# ---------------------------------------------------------------------------------------------------------------------------
# NUEVA CARGA DE DATAFRAME A SQL
# ---------------------------------------------------------------------------------------------------------------------------
def df_a_sql(conexion, n_server, df, n_bbdd, n_table, truncate = False, schema = 'dbo'):

    full_table_name = '{}.{}.{}'.format(n_bbdd, schema, n_table)
    
    if truncate:
        query_truncate = """
            TRUNCATE TABLE {};
        """.format(full_table_name)
        ejecuta_query(query_truncate)
    
    mssql_driver = get_mssql_driver().replace(' ', '+')
    '''
    conexion = sqlalchemy.create_engine('mssql+pyodbc://{}/{}?trusted_connection=yes&driver={}'.format(n_server, n_bbdd, mssql_driver),
                                    connect_args={'connect_timeout': 10}, echo=False)
    '''
    
    df.to_sql(con=conexion, schema= schema, name= n_table, if_exists = 'append', index=False, chunksize=1000)


#######################################################################################################################################
# LEYENDO UNA TABLA
#######################################################################################################################################

def sql_a_df(query,params = None):

    motor_sql = conecta_sql()
    conexion_sql = motor_sql.connect()

    df = read_sql_query(query,conexion_sql)
    
    conexion_sql.close()
    motor_sql.dispose()
    
    return df