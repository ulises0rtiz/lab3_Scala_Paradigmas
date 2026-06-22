# Laboratorio 3 - Procesamiento distribuido con Apache Spark

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Grafo de dependencias y flujo de datos

El flujo de datos del programa y los tipos de Scala involucrados en cada conexión se estructuran de la siguiente manera:

```text
[Driver] Archivo JSON local (subscriptions.json)
    │
    ▼ (FileIO.readSubscriptions)
[Driver] List[Subscription]
    │
    ▼ (sc.parallelize)
[RDD] RDD[Subscription]
    │
    ▼ (.map / .flatMap) Descarga de feeds y parseo de JSON
[RDD] RDD[Post] (Filtrados con title y selftext no vacíos)
    │
    ▼ (.flatMap) Extracción de entidades por post
[RDD] RDD[NamedEntity]
    │
    ▼ (.map) Generación de tuplas clave-valor
[RDD] RDD[((String, String), Int)] -> Estructura: ((entityType, text), 1)
    │
    ▼ (.reduceByKey) Agregación y suma de frecuencias (Barrera)
[RDD] RDD[((String, String), Int)] -> Estructura: ((entityType, text), total)
    │
    ▼ (.sortBy / .collect) Ordenamiento descendente y recolección
[Driver] List[((String, String), Int)] -> Impresión final por consola
```

### b) Mapeo de abstracciones Spark

- **Descarga y parseo de feeds (`RDD[Subscription] -> RDD[Post]`):** Se implementa con `flatMap`. Cada suscripción puede fallar (devuelve una lista vacía, `0` elementos) o completarse con éxito (devuelve una lista con `N` posts). Al ser una salida variable, requiere un `flatMap`.
- **Extracción de entidades (`RDD[Post] -> RDD[NamedEntity]`):** Se implementa con `flatMap`. Un post individual puede no contener ninguna entidad conocida o contener múltiples coincidencias (`0` a `N`).
- **Preparación del conteo (`RDD[NamedEntity] -> RDD[((String, String), Int)]`):** Se implementa con `map`. Transforma de manera lineal cada entidad en una tupla clave-valor con un contador base `1`.
- **Conteo y agregación:** Se implementa con `reduceByKey` para consolidar el total de apariciones de cada par (tipo, nombre).
- **Pasos que no encajan:** La lectura inicial del archivo de suscripciones (`subscriptions.json`), el procesamiento del comando `Dictionary.loadAll` (si se mantiene en el `Driver`) y el formateo de impresión final por pantalla. Estos pasos no encajan porque operan sobre el flujo secuencial del `Driver` o representan acciones terminales que cortan la evaluación perezosa (_lazy_) para materializar los datos en la memoria local.

### c) Barreras de sincronización

- **Pasos independientes:** La descarga de los archivos JSON por HTTP, el parseo de los posts, el filtrado morfológico de texto vacío y la ejecución del algoritmo de coincidencia de palabras contra el diccionario. Cada `Worker` procesa sus particiones asignadas de forma aislada sin intercambiar información con los demás.
- **Barreras de sincronización:** El operador `reduceByKey` y el ordenamiento global (`sortBy`). Actúan como una barrera porque exigen un proceso de _Shuffle_ (redistribución de datos por red). Ningún nodo puede calcular el ranking final ni el conteo definitivo de una entidad hasta que la totalidad de los `Workers` hayan terminado sus tareas de mapeo previas.

### d) Restricciones de las funciones distribuidas

Para que el framework pueda enviar el código del `Driver` a los `Workers` y ejecutarlo de forma distribuida, se deben cumplir las siguientes condiciones:

- **Serialización:** Todas las funciones anónimas (clausuras) y los objetos que estas arrastren adentro de transformaciones como `map` o `flatMap` deben implementar la interfaz `Serializable`. Si se intenta usar una clase o recurso no serializable, Spark lanza un error `TaskNotSerializableException` en tiempo de ejecución.
- **Inmutabilidad y Estado Aislado:** No se pueden usar variables mutables compartidas de ámbito global (como un `var` común de Scala) para acumular datos dentro del pipeline distribuido. Cada `Worker` corre en una JVM separada con su propia copia de la memoria; modificar una variable común solo afectará el entorno local de ese hilo en ese `Worker`, dejando al `Driver` con el valor inicial sin inmutarse. Las métricas compartidas se deben manejar únicamente mediante `Accumulators`.
- **Efectos secundarios localizados:** Las operaciones de entrada/salida como un `println` o la escritura de archivos dentro de un bloque distribuido se ejecutan en el contexto del `Worker`. Los mensajes impresos saldrán en los flujos estándar o logs de la máquina que procesa la tarea (o en la Spark UI en modo local), pero no se centralizarán automáticamente en la pantalla de la terminal del `Driver`.

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

- **¿Qué ocurre en el cluster en la barrera `reduceByKey` y por qué es inevitable?:** En ese punto ocurre un proceso llamado _Shuffle_. Los datos de los Workers se redistribuyen a través de la red para que todos los pares con la misma clave (la misma entidad nombrada) terminen en la misma partición y nodo para ser sumados. Es inevitable porque un Worker aislado no tiene la visibilidad global necesaria para emitir un conteo final definitivo; necesita combinar sus resultados parciales con los del resto del cluster.
- **Restricciones de la función en `reduceByKey`:** La función (en nuestro caso `_ + _`) debe cumplir obligatoriamente dos propiedades matemáticas: ser **conmutativa** ($a + b = b + a$) y **asociativa** ($(a + b) + c = a + (b + c)$). Spark puede ejecutar las reducciones parciales y el orden de la suma en distintos nodos y momentos impredecibles; si la operación dependiera del orden, el resultado sería inconsistente.
- **Lectura del diccionario de entidades:** El método `Dictionary.loadAll` se ejecuta de forma centralizada y secuencial en el **Driver** antes de paralelizar el procesamiento. Una vez materializada en una `List[NamedEntity]`, esta lista se incluye dentro de la clausura de la función del `flatMap` de los Workers, por lo que Spark la serializa y la envía por red a cada Worker de forma transparente.

## Ejercicio 4 — Monitoreo del éxito de las tareas

- **Uso exclusivo de Accumulators para métricas:** Los Accumulators no deben usarse para flujos de control lógico porque las tareas en un entorno distribuido pueden fallar y ser reintentadas silenciosamente por Spark, o reejecutarse si se pierde la memoria de un nodo. Si una tarea se reejecuta, el Accumulator volvería a incrementar su valor, generando duplicados y valores falsos e incorrectos. Son herramientas heurísticas de observabilidad, no de control de estado.
- **Disponibilidad de lectura del Accumulator:** El valor de un Accumulator solo es consistente y seguro de leer por el Driver **inmediatamente después de que se completa una Acción terminal** (como `.count()` o `.collect()`). Dado que las transformaciones son perezosas (_lazy_), si se intenta leer un Accumulator antes de una acción, su valor será `0`.
- **Comparativa de rendimiento (Local vs Distribuido):** Para un volumen bajo de datos (archivos simulados de pocos megabytes mediante un servidor local mock), la paralelización local (`local[*]`) no solo **no** mejora el tiempo de ejecución, sino que suele ser fraccionalmente más lenta que el esqueleto secuencial original.
  - _Justificación:_ El sobrecosto (_overhead_) de levantar la JVM de Spark, planificar el DAG, serializar funciones y orquestar el Shuffle supera el tiempo ahorrado por procesar simultáneamente tres feeds pequeños. La ventaja real del cómputo distribuido solo se aprecia al escalar masivamente el volumen de datos de entrada (Big Data), momento en el cual el entorno secuencial colapsaría por falta de memoria o tiempos excesivos, mientras que el entorno particionado asimilaría la carga horizontalmente.
