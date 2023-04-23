def t_datos(df_datos):
    df_datos['NOMBRE_COMPLETO'] = df_datos['APELLIDOS'] + ' ' + df_datos['NOMBRES']

    return df_datos