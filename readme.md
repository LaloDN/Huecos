# Script de huecos en horarios

## Contenido
1. [main](#main)
2. [get_environvar](#getenvironvar)
3. [database_connection](#databaseconnection)
4. [pm_counter](#pmcounter)
5. [sensors_id](#sensorsid)
6. [time_gaps](#timegaps)
7. [get_crudos](#getcrudos)
8. [generate_xml](#generatexml)
9. [post_xml](#postxml)

<a name="main"></a>

# Función main

## Region settings
Dentro de la función main, lo primero que se puede encontrar una serie de acciones previas que son necesarias para el script, como lo son la creación de una variable root, que contiene la ruta absoluta del script, el archivo de logs y los argumentos de argparse:

+ fecha: Es la fecha de los días en los que se va a buscar huecos dentro de la base de datos, este parámetro puede recibir multiples fechas separadas por una coma, las fechas se escriben en formato YYYYMMDD, si no recibe valores, por default tomará las fechas del día de ayer y de hoy.
+ mac: Es a dirección mac de un contador del cuál se va a analizar si tiene huecos, si este parámetro no recibe un valor, se analizarán los contadores de todas las plazas disponibles.
+ upload: Se utiliza para saber si se quiere postear la información a PogenData, solo se subirá si el usuario introduce un a 'y' o 'Y', si se introduce cualquier otro valor o si se deja en blanco, este en su lugar guardara los xml en un archivo de texto.
+ output: Es la dirección con la carpeta en donde se guardarán los archivos xml.

Después de esta región se crea una variable, inmediatamente se escribe la fecha y hora de cuando se ejecutó el script, y también se crea una variable solo con la hora de ejecución, esta variable servirá para crear una subcarpeta dentro de la carpeta donde se van a guardar los archivos xml, dentro de esta subcarpeta se van a almacenar todos los xml que se generen durante la ejecución del script

También están declarados dos diccionarios llamados down_config y aws_config, estos diccionarios contienen toda la información necesaria para aceder a las bases de datos, **por seguridad, todos los datos para establecer la conexión están encriptados dentro de un archivo .env encriptado**, dentro de este archivo .env escribirá todas las variables de entorno que sean necesarias para el script (ej: USER="my_user") y después tendrá que ser encriptado utilizando el comando gpg.
*(Nota: se recomienda crear este archivo .env dentro del mismo directorio del script, si no tendrá que modificar la función get_environvar)*.

## Region proceso
**Antes del proceso**
Todo el proceso principal está dentro de un ciclo for, el cuál se va a iterar sobre una lista de fechas, para eso, se toma el valor del argumento fecha, se separan los elementos por comas y se guardan en una lista.

**Paso 1**

Primero se transforma el valor actual del elemento que esta siendo iterado de la lista fechas, esta está en formato YYYYMMDD por lo que se guarda en la variable date con formato YYYY-MM-DD.

Una vez terminado con la declaración de variables y configuraciones necesarias, se procede con el primer paso que es obtener la lista de los contadores de la tabla con el mismo nombre en la base de datos, para eso se manda a llamar a la función pm_counter y recibe como parámero el argumento mac del argparse, para saber si este va a tener una única mac o si va a traer toda la información de los contadores disponibles, el resultado que traiga lo guardará en la variable contadores, que es una lista de objectos Contador.

Para saber si se puede seguir con el proceso, se consulta la longitud de la lista, si es igual a 0, no hay elementos que analizar, por lo que se procede con el siguiente elemento de la lista de fechas, si este no es el caso, entonces vale la pena seguir con el proceso.

**Paso 2**

El resto de las operaciones de aquí en adelante están dentro de otro loop en el que se va a iterar los registros de la lista contadores.

Al principio de cada iteración, se extraen los valores de las propiedades del objeto contador y se guardan en 3 variables seperadas: mac, num_serie, y contador.

Para el siguiente paso se necesita obtener la plaza id a la que está relacionada el contador, por lo que se ejecuta una sentencia SQL dentro del main, puede que un contador aún no tenga una plaza definida, por lo que preguntamos si el query nos trajo alguna coincidencia, en dado caso que este vacía, se procederá a la siguiente iteración de inmediato sin ejecutar nada mas.

***Nota: el siguiente paso ha sido removido por ser redundante***

**Paso 3**

Lo siguiente es obtener los valores de los campos acceso_id y sensor_id de un sensor en la tabla sensores, para esto nos apoyamos en la función sensors_id, que recibe como parámetro el número de serie y la plaza_id que obtuvimos anteriormente, los utilizará para filtrar los resultados en el query y finalmente, nos regresará estos valores en una tupla, los cuales se guardarán dentro del script en las variablaes acceso_id y sensor_id.

**Paso 4**

Puede ocurrir que los sensores actualizen su hora espontáneamente, por lo que puede brincar de ser las 14:59:59 y en el siguiente segundo pueden ser las 16:00:00, existiendo un hueco para la 15 horas, y pueden existir varios huecos de horas para un día en un sensor.
La función time_gaps obtendrá la lista de todos aquellas horas que no aparezcan en los registros, esta función recibirá la fecha y el id de la plaza en la que se van a buscar los huecos de horario, y el resultado lo guardará en la variable huecos, que es una lista que contiene valores como ['15:00:00','20:00:00'].

Puede que no existan huecos para una plaza en cierto día, por lo que con un if se comprueba si la lista contiene elementos, si no tiene nada dentro, entonces no continua con el paso 5 y procede con el siguiente registro de la lista contadores.

**Paso 5**

Una vez obtenida la lista de huecos de un sensor, lo que se tiene que hacer ahora es buscar el primer registro que aparezca en esa hora (ej: si tenemos un hueco a las 20 horas, se tendrá que buscar los registros que hayan sido creados a las 20 horas y tomar el valor del primer registro).

Para obtener los primeros registros de cada hora se utiliza la función get_crudos, en esta función le vamos a pasar la lista con las horas de huecos, la plaza id y la fecha en la que se están analizando los registros, dentro de esta función se ejecutará un query que iterará los registros de la lista huecos, obtendrá el primer registro de cada uno de ellos, esa información la transformará a un objeto Crudos, y ese mismo objeto lo guardará en una lista.
Al final de todas las iteraciones, retornará la lista con los registros y este valor se guardará en la variable records.

**Paso 6**

Una vez obtenidos los registros de las horas que aparecian como hueco, lo siguiente es generar es un archivo xml con estos registros.
La variable records es una lista, por lo que toca iterar los elementos que tiene en ella (que son objetos Crudos), para cada uno de los elementos, primero se manda a llamar la función generate_xml pasandole como parámetro el objeto de Crudos, dentro de esta función, se obtiene un JSON que está contenido dentro de las propiedades del objeto Crudos, este JSON se procesa para generar un string con el formato de un xml, generado apartir de la información del JSON. 

El valor de retorno de la función se guarda en una variable llamada xml_string, puede nos regrese un string con el formato xml, o puede que exista un error y nos retorne None, por lo que hay que preguntar si el valor es None, si se cumple la igualdad, se termina con el proceso y se procede a la siguiente iteración.
Pero en dado caso que xml_string resulte ser un string, hay dos operaciones que se pueden hacer con este dato en base al valor del argumento upload de argparse:
1. Si el valor del argumento es 'y' o 'Y', entonces se llamará a la función post_xml para enviar estos datos a pogen data.
2. Si el valor es cualquier otra cosa que no sea 'y' o 'Y' (o que incluso este vacío), se creará un archivo de texto con el contenido de la variable, en la carpeta con la ruta que contiene la variable que se creó en la region settings.

Después de esto, se termina con el proceso de un registro en la lista contadores y se itera el siguiente, y una vez que termine el ciclo for, se termina la ejecución del script.


<a name="getenvironvar"></a>

# Función get_environvar

> Recibe: var_name: un string con el valor de la variable de entorno a obtener.
> Retorna: un string con el valor de la variable de entorno.

Esta función ejecuta un comando de bash para desencriptar un archivo llamado .env.gpg que está ubicado dentro del mismo directorio del script y generá un archivo .env, con load_dotenv() se lee la información del archivo .env, se lee el valor de la variable con el nombre especifícado en var_name y lo retorna, por último elimina el archivo .env con el texto plano por seguridad.

***Nota: se puede cambiar la ubicación de en donde está contenido el archivo .env.gpg, pero tendrá que editarlo dentro de la función (y el nombre del archivo de texto plano .env no se puede cambiar, siempre tiene que ser el mismo)***


<a name="databaseconnection"></a>

# Función database_connection

> Recibe: args: un diccionario con los parámetros y valores necesarios para establecer una conexión a una base de datos.

> Retorna: una tupla con una conexión y un cursor de MySQL.

Toda esta función está contenida dentro de un bloque try, por lo que si algo llega a fallar a la hora de realizar la conexión, este interrumpirá el script y anotará los errores dentro del archivo de log.

## Dentro del try
Lo primero que se hace es crear la conexión a la base de datos con los parámetros de args y se comprueba con un if si la conexión funcionó, si no funciona, se lanza un mensaje de error a los logs y termina con la ejecución del script.

Pero si la conexión fue establecida con éxito, entonces ahora se procede a crear un cursor de la base de datos, después se ejecuta una consulta para ver que se haya seleccionado una base de datos correctamente, si la consulta no trae ningún resultado (None), se lanza un error a los logs y se termina con la ejecución del script, en caso contrario, significa que si se ha seleccionado la base de datos correctamente, por lo que retorna el cursor y la conexión una vez comprobada esta información.

<a name="pmcounter"></a>

# Función pm_counter

> Recibe: cursor: un cursor de una base de datos de MySQL, mac: un string con el valor del argumento mac del argparse.

> Retorna: una lista de objetos Contador.

Al principio se inicializa una variable llamada contadores, que es una lista de objetos Contador, y después se crea otra variable llamada query_string, con un query base: este query lo que hará es traernos toda la información de los contadores disponibles.
Pero es posible filtrar los resultados y reducir la búsqueda a un solo contador por su mac, para eso, con un if se pregunta si la variable mac no viene vacía, si contiene un valor, al query string se le concatena una nueva parte en donde se agergará la condicional para que traiga únicamente la info. de un contador.

Una vez establecido el valor de query_string, se ejecuta la consulta, se itera sobre las lineas de los resultados y cada registro lo va a adaptar y lo va a guardar a un objeto Contador, el cuál fue modelado para almacenar los valores de esta consulta, y cada objeto contador lo guarda en la lista contadores.
Al finalizar el ciclo for, la función retorna la lista de contadores.


<a name="sensorsid"></a>

# Función sensors_id

> Recibe: cursor: un cursor de una base de datos de MySQL, num_serie: un string con el numero de serie de un contador, plaza_id: un string del id de la plaza a la que esta ligada el sensor.

> Retorna: Una tupla con el valor de los campos acceso_id y sensor_id de la tabla sensores.

Esta función consta de una consulta hacía la tabla sensores, en donde traerá la información de los campos acceso_id y sensores_id de la tabla sensores, el cuál filtrara los resultados en donde tengan el mismo id de plaza que el del parámetro y el mismo numero se serie, además de que se encuentren activos.

Se ejecuta el query, se toma la primera línea del resultado y este se envía como valor de retorno.

<a name="timegaps"></a>

# Función time_gaps

> Recibe: cursor: un cursor de una base de datos de MySQL, date: un string de fecha con formato 'YYYY-MM-DD', plaza_id: un entero que contiene el id de la plaza ligada el contador.

> Retorna: una lista de strings con horas en formato 'H:M:S'.

Dentro de la función primero se crea una variable huecos, que será de tipo List[str] y estará inicializada con una lista vacía.

Para obtener los huecos de un día, se ejecuta un query de una plaza según su id y con los registros de el día que contenga el valor date, este query nos traerá las horas en las que estuvo encendido/activo el sensor, que van desde las 0 hasta las 23 horas, esta consulta se guarda en una variable llamada results.
Después se filtra únicamente el campo con la hora (que tendrá formato 'H:M:S') de los resultados, para eso se utilzia una compresión de lista y se guarda en otra variable llamda dates, y por último, se hace otra compresión de lista con dates, y esta vez se guardará unicamente el valor de las horas y se convertira a un entero, y el resultado se guardará en la variable hours, que se espera que tenga valores como [0,1,2,3,....,23].

Con un if se comprobará la longitud de la lista de hours, si tiene longitud 0, significa que el sensor estuvo apagado todo el día, se salta el proceso del if y se regresa la lista huecos vacía, pero en dado caso que tenga varias horas, iniciará el proceso para obtener un rango de horas de timestamp.

## Obteniendo las horas activas
Lo primero que se hace es ordenar la lista hours para que los elementos queden como [0,1,2,5,13...] para saber cuál fue la primera hora a la que se encendió el dispositivo y cuál fue la última, y así poder obtener un rango de horas. En la base de datos existe una hora llamada timestamp, la cuál esta 6 horas adelantada que la hora que utilizamos, por lo que para obtener el rango de horas en las que estuvo activo el dispositivo, se toma el primer elemento de la lista hours ya ordenada y se le suman 6 horas, se hace lo mismo con el último elemento de la lista, pero como los días solo tienen 24 horas y puede ser el caso que en nuestro horario normal sean las 23 horas y le sumamos 6, por lo que si un elemento es igual o sobrepasa las 24 horas, estas 24 horas se le restan para evitar incongruencias, estas dos horas nos servirán para los logs para monitorear en que horas de timestamp estuvo trabajando el dispositivo.

La operación para obtener los huecos es simple, con un for se va a iterar los números desde la primer hora de la lista hours hasta el último, en teoría, todos los números ese rango de horas tienen que estar todos las horas en la lista hours, pero si un número no está en la lista, significa que hubo un hueco, por lo que primero se le suman 6 horas para que tenga la hora de timestamp, después se comprueba si no excede las 24 horas, si es así, se le restan, y por último, ese número se le concatena una cadena como esta ':00:00' para que tenga el formato de hora en HH:MM:SS, y esta nueva cadena, se agrega a la lista huecos.

Al finalizar la iteración se retorna la lista huecos.

<a name="querydates"></a>

# Función query_dates
> Recibe: hour: un string de una hora en formato HH:MM:SS, date: un string de una fecha en formato YYYY-MM-DD
> Retorna: una tupla de dos strings de dos fechas distintas con un rango de una hora entre ellas

Dada una hora, esta función va a obtener un intervalo de tiempo en el que se englobe dicha hora.
Para empezar, del parámetro hour se extrae el primer valor antes de los dos puntos ':', este valor será la hora y deberá tener un número del 0 al 23 y se guarda en una variable llamada num, después de esto se crea un objeto datetime concantenando la fecha y la hora de los parámetros.

Supongamos que vamos a analizar los registros de un día cualquiera, por ejemplo, un día 26, dentro de la base de datos en la tablas suele haber dos horas: una es la hora que se utiliza en México y otra hora en timestamp, esta última esta adelantada 6 horas con la primera, por lo que si un día para nosotros es de 0 a 23 horas, en timpestamp sería de 6 a las 5 horas, y como después de las 23 horas siguen las 0 horas y se vuelve otro día, hay que verificar si la hora de la que se va a obtener un intervalo está de las 0 horas hasta las 5, si es así, vamos a añadirle un día al objeto datetime para que respete su hora en timestamp.

Una vez que se haya corroborado que la fecha y la hora del objeto datetime esta correcta, se procede a restarle un segundo al objeto y guardarlo en la variable first_hour y después, al objeto datetime se le suma una hora y se guarda en la variable second_hour, por último, estas variables se retornan en la función.

<a name="getcrudos"></a>

# Función get_crudos

> Recibe: cursor: un cursor de una base de datos de MySQL, huecos: una lista de strings con horas en formato "H:M:S", contador_id: el id del contador ,plaza_id: el id de la plaza a la que está ligada un contador, fecha: un string con la fecha de los registros que se están analizando.

> Retorna: una lista de crudos, con el primer registro de cada hora de los huecos.

Para iniciar, se crea una variable llamda records, que será de tipo List con objetos Crudos y estará inicilizada con una lista vacía.

Todo el proceso de la función está dentro de un ciclo for que iterará sobre los elementos de la lista huecos.
Primero se llama a la función query_dates a la que se le pasa como parámetro un el elemento de la lista huecos que va a contener una hora y el parámetro fecha, el resultado de la función se almacenará en las variables first_hour y second_hour que será un intervalo de tiempo en el que se engloban la hora que contiene la variable hueco, por ejemplo, si la variable tiene un valor de "13:00:00" la función retornará "12:59:59" y "14:00:00". 

Los valores de horas calculados, el contador_id, plaza_id y fecha se utilizan para ejecutar un query traerá los registros de la plaza que coincida con el id, de un contador, y que además, que los registros hayan sido creados entre el intervalo de tiempo de la fecha-hora que se forma con first_hour y second_hour.

Se toma únicamente el primer resultado de este query, y puede que no nos traiga resultados, lo que se interpreta como que no hubo crudos registrados en ese intervalo de tiempo, por lo que se escribe en los logs que no existe información para ese hueco, pero si el query nos trae información, entonces la información del query se va a guardar en un objeto Crudos, el cuál fue modelado para almacenar su información, y este objeto se va a agregar a la lista records. 
Después de este paso se términa con una iteracón del ciclo for, y una vez que se termine el ciclo, se retorna la lista records.

<a name="generatexml"></a>

# Función generate_xml

> Recibe: crudos: una instancia de la clase Crudos.

> Retorna: un valor None o un string que contiene un texto con formato xml.

En la clase crudos, existe una propiedad llamada json_text, recordemos que los objetos crudos se generan apartir de una consulta hacía la tabla registros_crudos, y hay una columna llamada json, y el valor de esta columna se utiliza para generar los archivos xml.

Primero se carga el valor de la propiedad json_text del objeto crudos y se parsea a un diccionario/JSON, este diccionario se manda a la función externa get_values_json, esta función nos trae otro diccionario, el cuál se guarda en la variable test, y dicha variable será enviada a otra función externa: generar_xml, se le pasan los datos necesarios y de valor de retorno, nos trae un string que contendrá un texto con un formato xml, este string se toma como valor de retorno de la función

> **Errores**
>: En ocasiones, en la base de datos en la tabla registros_crudos, los registros no tienen ningún valor en la columna json, por lo que el objeto Crudos generado apartir de este tampoco tenga la propiedad json_text, lo que generará un error y al final el valor de retorno de la función sea None.

<a name="postxml"></a>

# Función post_xml

> Recibe: xml_data: un string que contiene un texto en formato xml

> Retorna: Nada

Esta función se encarga de tomar el valor de la variable xml_data y codificarlo y guardarlo en una variable llamada data, seguido de eso se crea un objeto Request con la url a la que se va a hacer la petición y la data a enviar.
Una vez definido este objeto Request se hace una petición hacía PogenData, se verifica que la petición haya sido enviada con éxito, si es así, se escribe en los logs que la operación ha sido exitosa, y si no es así, se envían los datos de la respuesta de la petición hacía los archivos de los logs.



