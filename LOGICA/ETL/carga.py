from LOGICA.GENERAL.conexion import conecta_gs

def c_datos(df_datos):
    gs = conecta_gs()
    sp = gs.open('Registro')

    sh = sp.worksheet('Alumnos')
    sh.update('A2',df_datos.values.tolist())

