<img  align="left" width="150" style="float: left;" src="https://www.upm.es/sfs/Rectorado/Gabinete%20del%20Rector/Logos/UPM/CEI/LOGOTIPO%20leyenda%20color%20JPG%20p.png">
<img  align="right" width="60" style="float: right;" src="https://www.dit.upm.es/images/dit08.gif">


<br/><br/>

# Práctica: Procesamiento de streams con Apache Flink y Kafka (KRaft)

## 1. Objetivo de la práctica

En esta práctica aprenderemos a construir un **pipeline de procesamiento
de datos en tiempo real** utilizando:

-   **Apache Kafka (modo KRaft)** como sistema de ingestión de eventos.
-   **Apache Flink** como motor de procesamiento de streams.
-   **Scala** para implementar la lógica de procesamiento.

Al finalizar la práctica serás capaz de:

-   Leer eventos desde Kafka usando Flink
-   Transformar datos en streaming
-   Agrupar eventos por clave
-   Aplicar **ventanas temporales**
-   Calcular métricas en tiempo real

Duración estimada: **2 horas**

------------------------------------------------------------------------

## 2. Caso de uso

Imaginemos una empresa de **bicicletas compartidas en una ciudad**.

Cada vez que un usuario comienza o termina un trayecto, el sistema
genera un evento.

Ejemplo de evento:

``` json
{
 "bike_id":"bike_10",
 "station_id":"station_3",
 "user_id":"user_45",
 "event_type":"ride_start",
 "timestamp":1710000000
}
```

Estos eventos se envían continuamente a **Kafka**.

Nuestro sistema con **Flink** deberá:

1.  Leer los eventos desde Kafka
2.  Transformarlos a objetos Scala
3.  Filtrar los eventos de inicio de viaje
4.  Calcular **número de viajes por estación cada 30 segundos**

------------------------------------------------------------------------

## 3. Arquitectura de la práctica

    Event Producer
          |
          v
       Kafka (KRaft)
          |
          v
       Apache Flink
          |
          v
       Métricas en consola

------------------------------------------------------------------------

## 4. Preparación del entorno

El repositorio incluye un entorno Docker con:

-   Kafka (KRaft mode)
-   Flink JobManager
-   Flink TaskManager

------------------------------------------------------------------------

## 4.1 Levantar el entorno
Clonar el repositorio:
```bash
git clone https://github.com/Big-Data-ETSIT/PR_FLINK
```
```bash
cd PR_FLINK
```

Desde la carpeta `docker` del repositorio ejecutar:
```bash
cd docker
```

``` bash
docker compose up -d
```

Comprobar los contenedores:

``` bash
docker ps
```

Deberían aparecer contenedores similares a:

    kafka
    flink-jobmanager
    flink-taskmanager

------------------------------------------------------------------------

## 4.2 Acceder a la interfaz de Flink

Abrir en el navegador:

    http://localhost:8081

Esta interfaz permite visualizar los **jobs de streaming**.

------------------------------------------------------------------------

## 5. Crear el topic en Kafka

Entrar al contenedor de Kafka:

``` bash
docker exec -it kafka bash
```

``` bash
cd /opt/kafka/bin
```
Crear el topic:

``` bash
./kafka-topics.sh --create --topic bike-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Verificar que el topic existe:

``` bash
./kafka-topics.sh --list --bootstrap-server localhost:9092
```

Debería aparecer:

    bike-events

Salir del contenedor:

    exit

------------------------------------------------------------------------

## 6. Generar eventos de prueba

En otra terminal ejecutaremos un **producer simple** para enviar
eventos.

Entrar al contenedor:

``` bash
docker exec -it kafka bash
cd /opt/kafka/bin
```

Ejecutar el producer:

``` bash
./kafka-console-producer.sh --topic bike-events --bootstrap-server localhost:9092
```

Enviar manualmente algunos eventos:

``` json
{"bike_id":"bike_1","station_id":"station_1","user_id":"user_1","event_type":"ride_start","timestamp":1710000000}
{"bike_id":"bike_2","station_id":"station_2","user_id":"user_2","event_type":"ride_start","timestamp":1710000001}
{"bike_id":"bike_3","station_id":"station_1","user_id":"user_3","event_type":"ride_start","timestamp":1710000002}
```

Estos eventos serán consumidos por **Flink**.

------------------------------------------------------------------------

## 7. Implementación del job de Flink

El job se implementará en **Scala**.

Archivo:

    StreamingAnalytics.scala

------------------------------------------------------------------------

## Bloque 1 --- Crear el entorno de streaming

Primero creamos el **StreamExecutionEnvironment**.

Añadir el siguiente código:

``` scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

env.setParallelism(1)
```

Esto inicializa el entorno de ejecución de Flink.

------------------------------------------------------------------------

## Bloque 2 --- Leer eventos desde Kafka

Añadir el siguiente código para crear el **Kafka Source**.

``` scala
val kafkaSource = KafkaSource.builder[String]
  .setBootstrapServers("localhost:9092")
  .setTopics("bike-events")
  .setGroupId("flink-consumer")
  .setValueOnlyDeserializer(new SimpleStringSchema())
  .build()
```

Crear el stream:

``` scala
val stream = env.fromSource(
  kafkaSource,
  WatermarkStrategy.noWatermarks(),
  "Kafka Source"
)
```

------------------------------------------------------------------------

## Probar el funcionamiento

Añadir:

``` scala
stream.print()
```

Ejecutar el job.
cd flink-job

docker run -it --rm \
-v $(pwd):/workspace \
-w /workspace \
sbtscala/scala-sbt:openjdk-11.0.20_1.9.6_2.12.18 \
sbt run

Deberías ver en consola:

    {"bike_id":"bike_1","station_id":"station_1"...}

Si aparecen los eventos, **Flink está leyendo correctamente de Kafka**.

------------------------------------------------------------------------

## Bloque 3 --- Convertir JSON a objeto Scala

Definir la clase del evento:

``` scala
case class BikeEvent(
  bike_id: String,
  station_id: String,
  user_id: String,
  event_type: String,
  timestamp: Long
)
```

Ahora transformaremos el stream de `String` a `BikeEvent`.

Añadir:

``` scala
import org.json4s._
import org.json4s.jackson.JsonMethods._

implicit val formats = DefaultFormats
```

Transformación:

``` scala
val events = stream.map { json =>
  parse(json).extract[BikeEvent]
}
```

Probar:

``` scala
events.print()
```

------------------------------------------------------------------------

## Bloque 4 --- Filtrar eventos

Solo queremos contar **inicio de viajes**.

Añadir:

``` scala
val rideStarts = events.filter(
  _.event_type == "ride_start"
)
```

Probar:

``` scala
rideStarts.print()
```

------------------------------------------------------------------------

## Bloque 5 --- Agrupar por estación

Agrupamos los eventos por `station_id`.

``` scala
val stationStreams =
  rideStarts.keyBy(_.station_id)
```

Ahora convertimos cada evento en un contador.

``` scala
val stationCounts =
  stationStreams.map(event => (event.station_id, 1))
```

------------------------------------------------------------------------

## Bloque 6 --- Ventanas temporales

Ahora calcularemos **número de viajes cada 30 segundos**.

Añadir:

``` scala
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
```

Aplicar ventana:

``` scala
val tripsPerStation =
  stationCounts
    .keyBy(_._1)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
    .sum(1)
```

Mostrar resultados:

``` scala
tripsPerStation.print()
```

------------------------------------------------------------------------

## Bloque 7 --- Ejecutar el job

Finalmente ejecutar:

``` scala
env.execute("Bike Streaming Analytics")
```

------------------------------------------------------------------------

## 8. Resultado esperado

En la terminal de ejecución de flink Cada 30 segundos deberían aparecer resultados como:

    (station_1,5)
    (station_2,3)
    (station_3,1)

Esto indica **número de viajes iniciados por estación en los últimos 30
segundos**.

------------------------------------------------------------------------

## 9. Resumen del pipeline

El pipeline implementado es:

    Kafka
       ↓
    Flink Source
       ↓
    Parse JSON
       ↓
    Filter ride_start
       ↓
    KeyBy station
       ↓
    Window (30s)
       ↓
    Count

------------------------------------------------------------------------

## 10 Ejecutar todo en el cluster de flink 


## Simular eventos de entrada
Para esta parte de la práctica se va a hacer uso de un progrma que simula o genera evento de las biciletas y sus estaciones, una vez que se haya iniciado el job ahora hay que arrancer este programa para que los eventos sean capturados por kafka y enviados a flink:

Abra un nuevo terminal y diríjase al direcotio `producer`:
```bash
cd producer
```
luego inicie el programa
```bash
sbt run
```

En este punto debería tener una salida como la siguiente:

         
    [info] Enviado: {"bike_id":"bike_6","station_id":"station_1","user_id":"user_33","event_type":"ride_start","timestamp":1773171923202}
    [info] Enviado: {"bike_id":"bike_0","station_id":"station_3","user_id":"user_25","event_type":"ride_start","timestamp":1773171924205}
    [info] Enviado: {"bike_id":"bike_8","station_id":"station_3","user_id":"user_14","event_type":"ride_start","timestamp":1773171925210}
    [info] Enviado: {"bike_id":"bike_3","station_id":"station_0","user_id":"user_96","event_type":"ride_end","timestamp":1773171926216}

## Generar el jar y ejecutar el Job en Flink

Para poder ejecutar un job en un cluster externo (en este caso en docker) es necesario realizar cambios en el código para que apunten a la dirección donde se debe ejecutar el servicio. 

Primero en el Archivo:

    StreamingAnalytics.scala

cambiar la direcciñon de kafka por la del contenedor:

``` scala
val kafkaSource = KafkaSource.builder[String]
  .setBootstrapServers("kafka:29092")
  .setTopics("bike-events")
  .setGroupId("flink-consumer")
  .setValueOnlyDeserializer(new SimpleStringSchema())
  .build()
```
 Una vez realizado en el códgio debemos generar el archivo .jar que se va a subir al cluster de flink. Para ello abra un terminar y dirñijase a la carpeta `flink-job` y ejecute la siguiente intrucción:

 ```bash
 sbt assembly
 ```
este comando permitirá generar lo que se denomina un fat jar o un fichero con todas las dependencias que encesita el programa ya empaquetas en ese fichero.

### Subir el job a Flink

Abrir en el navegador:

    http://localhost:8081

Diríjase a la opción de submit new job y suba el fichero `flink-streaming-analytics-assembly-0.1.jar` que se encuentra dentro del directorio `target` de `flink-job`

En la pestaña de logs del Job Manger navegador puede ver la ejecución del Job

y para visualizar la salida del programa abra un nuevo terminal y vea los logs del contenedor del taskmanager:

```bash
docker logs -f flink-taskmanager
```
------------------------------------------------------------------------

# 11. Desafío opcional (ejercicio final)

## Flink Complex Event Processing CEP con Kafka

Este ejemplo ejecuta una aplicación completa de Flink en un clúster Dockerizado que contiene Apache Kafka (en modo KRaft), un Flink JobManager y un Flink TaskManager. Demuestra **Procesamiento de Eventos Complejos (CEP)** detectando un patrón específico de eventos dentro de un flujo continuo de datos.

### Cómo ejecutar el ejemplo

1. Tener iniciado el broker de Kafka y el clúster de Flink, en caso de no tenerlo realizar lo siguiente:

   ```bash
   docker-compose up -d
   ```

2. **Construir el Fat JAR de la aplicación de Flink:**
   Flink requiere que todas las dependencias se empaqueten en un único archivo JAR para enviarlo al clúster.

   ```bash
   sbt assembly
   ```

3. **Desplegar el Job en Flink:**
   Puedes enviar el job mediante la interfaz web de Flink en [http://localhost:8081](http://localhost:8081) subiendo el JAR ubicado en `target/scala-2.12/flink-scala-examples-assembly-0.1.jar` y especificando la clase de entrada `org.example.cep.FlinkCepLoginAlerts`.

   Alternativamente, desplégalo directamente mediante la CLI de Docker:

   ```bash
   # Copiar el JAR dentro del contenedor JobManager
   docker cp target/scala-2.12/flink-scala-examples-assembly-0.1.jar jobmanager:/opt/flink/usrlib/
   
   # Ejecutar el job de Flink
   docker exec jobmanager flink run -d -c org.example.cep.FlinkCepLoginAlerts /opt/flink/usrlib/flink-scala-examples-assembly-0.1.jar
   ```

4. **Iniciar la fuente de datos falsa:**
   En una nueva ventana de terminal, ejecuta el programa en Scala que simula usuarios iniciando sesión. Este publica mensajes JSON en el tópico de Kafka `login-events`.

   ```bash
   sbt "runMain org.example.cep.FakeKafkaSource"
   ```

5. **Observar la salida:**
   Observa los logs del Flink TaskManager. Cuando `user_bob` falle al iniciar sesión 3 veces seguidas, la lógica de CEP detectará el patrón y generará un `AlertEvent`.

   ```bash
   docker logs -f taskmanager
   ```

   *Busca líneas que comiencen con `ALERT> AlertEvent(user_bob,...`*

6. **Limpiar:**
   Cuando termines, apaga los contenedores de Docker para liberar recursos.

   ```bash
   docker-compose down
   ```

#### Cómo funciona la lógica de CEP

El núcleo de este ejemplo se encuentra dentro del directorio `cep` del repositorio clonado en `src/main/scala/org/example/cep/FlinkCepLoginAlerts.scala`. El Procesamiento de Eventos Complejos permite a Flink analizar un flujo de eventos individuales y encontrar secuencias que coincidan con un patrón predefinido.

1. **Watermarks y Timestamps:**  
   Al flujo se le asignan timestamps basados en el campo `timestamp` del JSON. Esto garantiza que Flink respete la semántica de tiempo de evento, incluso si los mensajes llegan fuera de orden.

2. **Particionamiento del flujo:**  
   Usamos `.keyBy(_.userId)` para que el patrón CEP se evalúe de manera independiente para cada usuario. Un fallo de inicio de sesión de Alice no contará para el conteo de fallos de Bob.

3. **Definición del patrón:**  
   Definimos un `Pattern` de Flink usando la API de Java:
   - `Pattern.begin("first").where(event => !event.success)`: Buscar un inicio de sesión fallido.
   - `.next("second").where(...)`: Buscar un **segundo** inicio de sesión fallido *estrictamente consecutivo*.
   - `.next("third").where(...)`: Buscar un tercer inicio de sesión fallido consecutivo.
   - `.within(Time.seconds(10))`: Los tres eventos deben ocurrir dentro de una ventana de 10 segundos.

4. **Selección de coincidencias:**  
   Cuando el patrón coincide, la función `.select()` extrae los eventos coincidentes y genera un `AlertEvent` que describe la anomalía.


## Qué hemos aprendido

En esta práctica hemos utilizado:

-   Kafka como **sistema de ingestión**
-   Flink como **motor de streaming**
-   Transformaciones de **DataStream**
-   **keyBy**
-   **windowing**
-   Procesamiento en **tiempo real**
-   Procesamiento de Eventos complejos CEP
