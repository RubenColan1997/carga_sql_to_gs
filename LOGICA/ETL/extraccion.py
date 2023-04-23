from LOGICA.GENERAL.funciones import sql_a_df
from LOGICA.GENERAL.constantes import Q_ALUMNOS

def e_datos():
    df_datos = sql_a_df(Q_ALUMNOS)

    return df_datos
