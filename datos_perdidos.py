from mysql.connector import connect, Error, connection,cursor
from datetime import datetime, timedelta
from dateutil.parser import parse
from dotenv import load_dotenv
from pprint import pprint as pp
from pydantic import BaseModel
from typing import Tuple,List,Dict, Union
import os,logging, argparse
import json
class Crudos(BaseModel):
    id: int
    contador_id: int
    registros: str 
    fecha_utc: datetime
    created: datetime
    plaza_id: int
    json_text: Union[str,None]

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
        if connection.is_connected():
            cursor=connection.cursor()
            cursor.execute('select database();')
            result=cursor.fetchone()
            if result[0] is None:
                logging.critical('No se ha podido conectar a la base de datos indicada.')
                logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
                quit()
            return connection,cursor
        logging.critical('No se ha podido establecer la conexión al host indicado.')
        logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
    except Error as e:
        print('Ha ocurrido algo inesperado en la conexión a la base de datos, terminando la ejecución del script...')
        logging.critical(str(e))
        logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
        quit()
        
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
        
def sensors_id(cursor: cursor.MySQLCursor, num_serie: str, plaza_id: str) -> Tuple[int,int]:
    """Obtiene los valores del campo de acceso id y sensores id de un sensor"""
    cursor.execute('select sensores.acceso_id as acceso_id, sensores.id as sensor_id from sensores'
                ' left join accesos on accesos.id = sensores.acceso_id'
                f' where pointmanager_id = {plaza_id} and sensor = "{num_serie}" and sensores.activo = 1')
    results=cursor.fetchone()
    return results       
     
def time_gaps(cursor: cursor.MySQLCursor, date: str, plaza_id:int) -> List[str]:
    """Función para encontrar huecos de horas"""
    huecos : List[str] = []
    dates : List[datetime]
    try:
        cursor.execute('select acceso_id,plaza_id,sensor_id,fecha, timestamp from datos'
                        ' left join data on datos.data_id = data.data_id'
                        f' where plaza_id = {plaza_id} and date(fecha) = "{date}";')
        results=cursor.fetchall()
        #Primero se obtiene el campo de la fecha y hora
        dates=[result[4] for result in results]
        #Después se obtiene únicamente las horas
        hours=[int(date.strftime('%H')) for date in dates]
        for i in range(0,24):
            if i not in hours:
                huecos.append(str(i)+':00:00')
    except Exception as e:
        logging.critical(str(e))
    finally:
        return huecos 
             
def get_crudos(cursor: cursor.MySQLCursor, huecos: List[str], contador_id: int, plaza_id: int, fecha: str) -> List[Crudos]:
    """Función para obtener los registros perdidos de ciertos intervalos de horas"""
    records : List[Crudos] = []
    for hueco in huecos:
        hour=parse(hueco)
        first_hour=(hour-timedelta(minutes=1)).strftime('%H:%M:%S')
        secound_hour=(hour+timedelta(minutes=60)).strftime('%H:%M:%S')
        cursor.execute('select * from pogen_contadores.registros_crudos'
                        f' where contador_id = {contador_id} and plaza_id = {plaza_id}'
                        f' and fecha_utc > "{fecha} {first_hour}" and fecha_utc < "{fecha} {secound_hour}"')
        result=cursor.fetchone()
        c=Crudos(id=result[0],contador_id=result[1],registros=result[2],fecha_utc=result[3],created=result[4],plaza_id=result[5],json_text=result[6])
        records.append(c)
    return records
 
def generate_json(crudos: Crudos, num_serie: str, mac_addres: str):
    query_json='{"macAddress": "'+mac_addres+'", "SiteName": "DefaultSiteName", "IPv4Address": "192.168.1.54", "FriendlyDeviceSerial": "'+num_serie+'", "DeviceName": "DefaultName", "TimeZone": "America/Mexico_City", "datos": {datetime.datetime(2022, 6, 3, 20, 20): {"registros":'+crudos.registros+', "salida": 62}}, "tipo": None, "ClockChanged": "2022-06-03T19:55:16Z*2022-06-03T20:15:28Z"}'
    my_js=json.loads(query_json)
 
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
        'database':get_environvar('down_db'),
        'connection_timeout':15
    }
    
    aws_config={
        'user':get_environvar('aws_user'),
        'password':get_environvar('aws_password'),
        'host':get_environvar('aws_host'),
        'database':get_environvar('aws_db'),
        'connection_timeout':15
    }
    #endregions
    
    #region database connection
    logging.info('Estableciendo conexión hacía PogenDown a la base de datos pogen_contadores...')
    con_d,cur_d=database_connection(down_config)
    logging.info('Conexión establecida con éxito.')
    logging.info('Estableciendo conexión hacía PogenAWS a la base de datos 650557_pogen..')
    con_a,cur_a=database_connection(aws_config)
    logging.info('Conexión establecida con éxito.')
    logging.info('Recuperando datos de la tabla contadores...')
    #endregion
    
    contadores=pm_counter(cur_d,args.mac)
    if contadores == None:
        logging.warning('No se han encontrado resultados del query para la tabla contadores para la mac '+args.mac)
        logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
        quit()
    mac=contadores[0]
    num_serie=contadores[1]
    contador_id=contadores[2]
    logging.info('Datos recuperados sin problemas.')
    cur_a.execute(f'select foreign_key as plaza_id from equipos where numero_de_serie = "{mac}" and activo = 1;')
    result=cur_a.fetchone()
    plaza_id=result[0]
    acceso_id, sensor_id=sensors_id(cur_a,num_serie,plaza_id) #paso 3 
    date= datetime.now().strftime('%Y-%m-%d') if args.fecha=='' else args.fecha[:4]+'-'+args.fecha[4:6]+'-'+args.fecha[6:]
    huecos=time_gaps(cur_a,date,plaza_id) #Paso 4, nota, el paso 5 ya lo tenemos con contador_id
    
    if len(huecos)>0:
        records=get_crudos(cur_d,huecos,contador_id,plaza_id,date) #paso 6
        generate_json(records[0],num_serie,mac)
        
    else:
        logging.info('No se han encontrado huecos entre las horas para el sensor '+args.mac+' del día '+date)
        
    con_d.close()
    con_a.close()
    logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')

if __name__ == '__main__':
    main()