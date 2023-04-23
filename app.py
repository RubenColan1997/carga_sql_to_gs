from LOGICA.ETL.extraccion import e_datos
from LOGICA.ETL.transformacion import t_datos
from LOGICA.ETL.carga import c_datos
from time import time

def main():
    time_0 = time()
    df_datos = e_datos() 
    print('Tiempo extraccion: ' , round(time()-time_0,2))

    time_0 = time()
    df_datos = t_datos(df_datos)
    print('Tiempo transformacion: ' , round(time()-time_0,2))

    time_0 = time()
    c_datos(df_datos)
    print('Tiempo carga: ' , round(time()-time_0,2))

if __name__ == '__main__':
    main()
    