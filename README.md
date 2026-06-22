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

Para compilar el código fuente y ejecutar el pipeline completo con un único comando, utilice el Makefile incluido en la raíz del proyecto. Este archivo ya se encarga de inyectar las variables de entorno necesarias para Java 17:

```bash
make
```
