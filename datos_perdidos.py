from unicodedata import name
from mysql.connector import connect, Error, connection,cursor
from datetime import datetime
from dotenv import load_dotenv
from typing import Tuple,List,Dict
import os,logging, argparse

from numpy import arange

def get_environvar(var_name: str)->str:
    """Obtiene el valor de las variables de entorno sensibles"""
    try:
        root=os.path.dirname(os.path.realpath(__file__))
        os.system('gpg '+root+'/.env.gpg')
        load_dotenv()
        variable=os.environ.get(var_name)
        if os.path.exists(root+'/.env'):
            os.remove(root+'/.env')
        return variable
    except:
        print('Error: no ocurrido algo inesperado al buscar la variable de entorno '+var_name)
        return ''
        
def database_connection(args=Dict[str,str])-> Tuple[connection.MySQLConnection,cursor.MySQLCursor]:
    """Obtiene la conexión de mysql y el cursor de una base de datos"""
    try:
        connection=connect(**args)
        cursor=connection.cursor()
        return connection,cursor
    except Error as e:
        print(e)
        
def pm_counter(cursor: cursor.MySQLCursor, mac:str) -> Tuple[str,str,int]:
    try:
        cursor.execute('SELECT pm_simulados.mac, contadores.num_serie, contadores.id as contador_id FROM pogen_contadores.pm_simulados_contadores'
                        ' left join pogen_contadores.pm_simulados on pm_simulados.id = pm_simulados_contadores.pm_simulado_id'
                        ' left join pogen_contadores.contadores on contadores.id = pm_simulados_contadores.contador_id'
                        f' where contadores.tipo = "4d" and pm_simulados.mac = "{mac}"')
        result=cursor.fetchone()
        return result
    except Error as e:
        logging.critical(str(e))

def main():
    
    #region settings
    root=os.path.dirname(os.path.realpath(__file__))
    logging.basicConfig(level='DEBUG',filename=os.path.join(root,'app.log'))
    parser=argparse.ArgumentParser(description='Script para analizar huecos de datos')
    parser.add_argument('--fecha',help='Fecha de los registros a analizar dentro de la base de datos', type=str,nargs='?',const=1,default='')
    parser.add_argument('--mac',help='Dirección mac del sensor que se quiere analizar',type=str)
    args=parser.parse_args()
    #endregion
    
    logging.info('\n-------------------------------\nFecha y hora de ejecución: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    #region database config
    down_config={
        'user':get_environvar('down_user'),
        'password':get_environvar('down_password'),
        'host':get_environvar('down_host'),
        'database':get_environvar('down_db')
    }
    
    aws_config={
        'user':get_environvar('aws_user'),
        'password':get_environvar('aws_password'),
        'host':get_environvar('aws_host'),
        'database':get_environvar('aws_db')
    }
    #endregions
    
    con,cur=database_connection(down_config)
    contadores=pm_counter(cur,args.mac)
    if contadores:
        print(contadores)
    con.close()
    
    
    logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')

if __name__ == '__main__':
    main()