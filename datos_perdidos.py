from mysql.connector import connect, Error, connection,cursor
from datetime import datetime, timedelta
from urllib import request as url_request
from dateutil.parser import parse
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Tuple,List,Dict, Union
import os,logging, argparse, json
from generacion_xmls import generar_xmls, get_values_json

class Crudos(BaseModel):
    """Clase para guardar los atributos de la consulta de la tabla crudos"""
    id: int
    contador_id: int
    plaza_id: int
    fecha_utc: datetime
    created: datetime
    registros: str 
    json_text: Union[str,None] = None
    
class Contador(BaseModel):
    """Clase para guardar los atributos de la consulta de la tabla contador"""
    mac: str
    num_serie: str
    contador_id: int
    
def get_environvar(var_name: str)->str:
    """Obtiene el valor de las variables de entorno sensibles"""
    try:
        root=os.path.dirname(os.path.realpath(__file__))
        #os.system('gpg '+root+'/.env.gpg')
        load_dotenv()
        variable=os.environ.get(var_name)
        #if os.path.exists(root+'/.env'):
            #os.remove(root+'/.env')
        return variable
    except:
        print('Error: no ocurrido algo inesperado al buscar la variable de entorno '+var_name)
        return ''
        
def database_connection(args=Dict[str,str])-> Tuple[connection.MySQLConnection,cursor.MySQLCursor]:
    """Obtiene la conexión de mysql y el cursor de una base de datos"""
    try:
        connection=connect(**args)
        if connection.is_connected():
            cursor=connection.cursor(buffered=True)
            cursor.execute('select database();')
            result=cursor.fetchone()
            if result[0] is None:
                logging.critical('No se ha podido conectar a la base de datos indicada.')
                logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
                quit()
            return connection,cursor
        logging.critical('No se ha podido establecer la conexión al host indicado.')
        logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
        quit()
    except Error as e:
        print('Ha ocurrido algo inesperado en la conexión a la base de datos, terminando la ejecución del script...')
        logging.critical(str(e))
        logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
        quit()
        
def pm_counter(cursor: cursor.MySQLCursor, mac:str) -> List[Contador]:
    """Obtiene valores de la tabla contadores"""
    contadores: List[Contador] = []
    query_string='SELECT pm_simulados.mac, contadores.num_serie, contadores.id as contador_id FROM pogen_contadores.pm_simulados_contadores'\
                        ' left join pogen_contadores.pm_simulados on pm_simulados.id = pm_simulados_contadores.pm_simulado_id'\
                        ' left join pogen_contadores.contadores on contadores.id = pm_simulados_contadores.contador_id'\
                        f' where contadores.tipo = "4d"'
    if mac!='':
        query_string+=f' and pm_simulados.mac="{mac}"'
        
    try:
        cursor.execute(query_string)
        results=cursor.fetchall()
        for result in results:
            contador=Contador(mac=result[0],num_serie=result[1],contador_id=result[2])
            contadores.append(contador)
    except Error as e:
        logging.critical(str(e))
    finally:
        return contadores
        
def sensors_id(cursor: cursor.MySQLCursor, num_serie: str, plaza_id: int) -> Tuple[int,int]:
    """Obtiene los valores del campo de acceso id y sensores id de un sensor"""
    try:
        logging.info('Obteniendo el acceso_id y sensor_id...')
        cursor.execute('select sensores.acceso_id as acceso_id, sensores.id as sensor_id from sensores'
                    ' left join accesos on accesos.id = sensores.acceso_id'
                    f' where pointmanager_id = {plaza_id} and sensor = "{num_serie}" and sensores.activo = 1')
        results=cursor.fetchone()
        logging.info('Valores obtenidos sin problemas')
        return results   
    except Error as e:
        logging.critical(' Error en la función sensors_id: '+str(e))
        return -1,-1   
     
def time_gaps(cursor: cursor.MySQLCursor, date: str, plaza_id:int) -> List[str]:
    """Función para encontrar huecos de horas"""
    huecos : List[str] = []
    dates : List[datetime]
    try:
        logging.info('Obteniendo lista de los huecos para el día '+date)
        cursor.execute('select acceso_id,plaza_id,sensor_id,fecha, timestamp from datos'
                        ' left join data on datos.data_id = data.data_id'
                        f' where plaza_id = {plaza_id} and date(fecha) = "{date}";')
        results=cursor.fetchall()
        #Primero se obtiene el campo de la fecha y hora
        dates=[result[3] for result in results] 
        #Después se obtiene únicamente las horas
        hours=[int(date.strftime('%H')) for date in dates]
        if len(hours)>0:
            hours.sort()
            first_hour=hours[0]+6
            last_hour=hours[-1]+6
            if first_hour>=24:
                first_hour-=24
            if last_hour>=24:
                last_hour-=24
            logging.info('Primera hora (timestamp) detectada: '+str(first_hour))
            logging.info('Última hora (timestap) detectada: '+str(last_hour))
            for i in range(hours[0],hours[-1]):
                if i not in hours:
                    timestamp=i+6
                    if timestamp>=24:
                        timestamp-=24
                    huecos.append(str(timestamp)+':00:00')
        logging.info('Se ha terminado el análisis para los huecos.')
    except Exception as e:
        logging.critical(str(e))
    finally:
        return huecos 
             
def query_dates(hour: str,date: str) -> Tuple[str,str]:
    """Función para obtener las fehas y las horas para ejecutar el query de get_crudos"""
    num=int(hour.split(':')[0])
    date_dt=parse(date+' '+hour)
    # Las horas de 0 a 5 son del día siguiente en el timestamp.
    if num >=0 or num <=5:
            date_dt+=timedelta(days=1)
    first_hour=(date_dt-timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
    second_hour=(date_dt+timedelta(minutes=60)).strftime('%Y-%m-%d %H:%M:%S')
    return first_hour,second_hour
          
def get_crudos(cursor: cursor.MySQLCursor, huecos: List[str], contador_id: int, plaza_id: int, fecha: str) -> List[Crudos]:
    """Función para obtener los registros perdidos de ciertos intervalos de horas"""
    records : List[Crudos] = []
    for hueco in huecos:
        try:
            first_hour,second_hour=query_dates(hueco,fecha)
            cursor.execute('select * from pogen_contadores.registros_crudos'
                            f' where contador_id = {contador_id} and plaza_id = {plaza_id}'
                            f' and fecha_utc > "{first_hour}" and fecha_utc < "{second_hour}"')
            result=cursor.fetchone()
            if result is not None:
                logging.info('Obteniendo el primer registro para la hora '+hueco)
                c=Crudos(id=result[0],contador_id=result[1],registros=result[2],fecha_utc=result[3],created=result[4],plaza_id=result[5],json_text=result[6])
                records.append(c)
                logging.info('Registro complteado!')
        except Exception as e:
            logging.critical('Error en la función get_crudos: '+str(e))
    if len(records)==0:
        logging.info('No se encontraron datos registrados para las horas que tienen hueco.')
    return records
 
def generate_xml(crudos: Crudos) -> Union[str,None]:
    """Función para generar el xml para enviar a PogenData"""
    try:
        logging.info('Obteniendo json del objeto '+repr(crudos))
        if crudos.json_text is None:
            logging.warning('El objeto no tiene ningún valor para la propiedad json_text')
            return None
        full_dict=json.loads(crudos.json_text)
        test = get_values_json(full_dict)
        logging.info('Json y valores generados con éxito!')
        logging.info('Iniciando la generación del xml...')
        for fecha, conteo in test["datos"].items():
            tmp = generar_xmls(conteo, test, fecha)
        logging.info('Se ha generado el xml de manera satisfactoria!')
        return tmp
    except Exception as e:
        logging.critical('Error en la función generate_xml: '+str(e))

def post_xml(xml_data: str, ocupacion=0, plaza_id=None)-> None:
    try:
        if ocupacion == 0:
            url = "http://www.sistema.pogen.mx/parserv1.php"
        else:
            url = "http://xmls.pogendata.com/xml"
        logging.info('Hora de la opreación '+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        if plaza_id:
            logging.info('Envíando xml de la plaza '+plaza_id+' hacía la url '+url)
        else:
            logging.info('Envíando el xml hacía la url '+url)
        data = str.encode(xml_data)
        conn = url_request.Request(url=url, data=data)
        resp = url_request.urlopen(conn)
        status_code = resp.getcode()
        respuesta = resp.read().decode("UTF-8")
        if status_code == 200 and "Download Complete" in respuesta:
            logging.info('Archivo XML envíado con éxito!')
        else:

            logging.warning('Ha ocurrido algo inesperado en la petición al servidor')
            logging.warning('Status code: '+str(status_code))
            logging.warning('Respuesta de la petición:' +str(respuesta))
            logging.warning('XML envíado: \n'+xml_data)
    except Exception as e:
        logging.critical('Ha ocurrido un error en la función post_xml: '+str(e))

def main():
    
    #region settings
    root=os.path.dirname(os.path.realpath(__file__))
    logging.basicConfig(level='DEBUG',filename=os.path.join(root,'app.log'))
    parser=argparse.ArgumentParser(description='Script para analizar huecos de datos')
    today=datetime.now()
    yesterday=(today-timedelta(days=1)).strftime('%Y%m%d')
    today=today.strftime('%Y%m%d')
    parser.add_argument('--fecha',help='Fecha de los registros a analizar dentro de la base de datos', type=str,nargs='?',const=1,default=today+','+yesterday)
    parser.add_argument('--mac',help='Dirección mac del sensor que se quiere analizar',type=str,nargs='?',const=1,default='')
    parser.add_argument('--upload',help="Si quiere que los resultados se suban a PogenData introduzca 'y' o 'Y', si no necesita subir datos introduzca cualquier otra cosa o deje en el argumento blanco ",
                        type=str,nargs='?',const=1,default='n')
    parser.add_argument('--output',help='Carpeta en donde se van a generar los archivos xml',type=str,nargs='?',const=1,default=os.path.join(root,'xml'))
    args=parser.parse_args()
    #endregion
    
    logging.info('\n-------------------------------\nFecha y hora de ejecución: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    execution_hour=datetime.now().strftime('%H:%M:%S')
    dirname=os.path.join(args.output,datetime.now().strftime('%Y-%m-%d'),execution_hour)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
        
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
    #endregion
        
    #region Proceso
    fechas:List[str]=args.fecha.split(',')
    for f in fechas:
        date=f[:4]+'-'+f[4:6]+'-'+f[6:]
        logging.info('\n\nInicando análisis para la fecha '+date)
        logging.info('Recuperando datos de la tabla contadores...')
        contadores=pm_counter(cur_d,args.mac) #paso 1
        logging.info('Datos recuperados sin problemas.')

        if len(contadores) == 0:
            logging.warning('No se han encontrado resultados del query para la tabla contadores para los parámetros especificados')
            continue
            
        for contador in contadores:
            #region obtención huecos
            mac=contador.mac
            num_serie=contador.num_serie
            contador_id=contador.contador_id
            logging.info('\n\nObteniendo el id de la plaza para el contador '+mac)
            cur_a.execute(f'select foreign_key as plaza_id from equipos where numero_de_serie = "{mac}" and activo = 1;') #paso 2
            result=cur_a.fetchone()
            if result is None:
                logging.warning('No existe un id plaza para el contador '+mac)
                continue
            plaza_id=result[0]
            #acceso_id, sensor_id=sensors_id(cur_a,num_serie,plaza_id) #paso 3 
            huecos=time_gaps(cur_a,date,plaza_id) #Paso 4, nota, el paso 5 ya lo tenemos con contador_id
            #endregion
            
            #region xml
            if len(huecos)>0:
                logging.info('Se han encontrado varios huecos de hora para el día '+date+': '+str(huecos))
                filename=mac+' '+args.fecha+'.txt'
                
                records=get_crudos(cur_d,huecos,contador_id,plaza_id,date) #paso 6
                for record in records:
                    xml_string=generate_xml(record)
                    if xml_string is not None:
                        if args.upload=='Y' or args.upload=='y':
                            post_xml(xml_string)
                        else:
                            if os.path.exists( os.path.join(dirname,filename) ):
                                f=open(os.path.join(dirname,filename),"a")
                            else:
                                f=open(os.path.join(dirname,filename),"w")
                            logging.info('Agregando info. al archivo '+dirname+'/'+filename+'...')
                            f.write('\n '+xml_string)
                            logging.info('Información agregada sin problemas')
                            f.close()
                #Limpieza de datos
                records.clear()
            else:
                logging.info('No se han encontrado huecos entre las horas para el sensor '+mac+' del día '+date)
                
            #Limpieza de datos
            huecos.clear()
            #endregion

        
    #endregion   
     
    #region end
    logging.info('Cerrando conexiones de bases de datos...')
    con_d.close()
    con_a.close()
    logging.info('Finalizando la ejecución del script...')
    logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
    #endregion

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.critical('Ha ocurrido un error que ha detenido la ejecución del script')
        logging.critical(str(e))
        logging.info('\nFecha y hora de finalización: '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n-------------------------------\n\n')
