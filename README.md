# Laboratorio 3 - Paradigmas de Programación (FAMAF)

## Procesamiento Distribuido con Apache Spark

Este proyecto implementa un pipeline de procesamiento distribuido usando Apache Spark y Scala para descargar, filtrar y extraer entidades nombradas (NER) de posts de Reddit, utilizando el paradigma Map-Reduce.

### Requisitos Previos

- **Scala & sbt:** Herramientas de compilación.
- **Java 17:** Obligatorio para la compatibilidad del entorno de Apache Spark 3.x. El proyecto no funcionará correctamente bajo Java 21+.

### Configuración del Entorno (Modo Local)

El programa está configurado para correr usando una SparkSession en modo local (`local[*]`), utilizando todos los núcleos disponibles de la máquina.

Los feeds a procesar se definen en el archivo `data/valid_subscriptions.json`. Para evitar bloqueos (Error 429) por parte de la API real de Reddit, las URLs de este archivo deben apuntar al mock server local provisto por la cátedra (`http://localhost:8123`).

### Ejecución

Para probar el proyecto de forma local, es necesario utilizar **dos terminales** simultáneas:

**Paso 1: Levantar el servidor Mock (Terminal 1)**
En la primera terminal, dirígete a la carpeta del servidor local falso y ejecútalo. Este paso es obligatorio para simular las descargas HTTP.

```bash
cd reddit-mock/
sbt run
```
