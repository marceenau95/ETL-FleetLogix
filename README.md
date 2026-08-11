# ETL-FleetLogix
----
FleetLogix es una empresa de transporte y logística enfocada en operaciones de última milla, con una flota aproximada de 200 vehículos distribuidos en cinco ciudades principales. La organización operaba inicialmente mediante sistemas legacy y procesos manuales soportados en hojas de cálculo, lo cual limitaba la capacidad de análisis, monitoreo operativo y toma de decisiones en tiempo real.
Con el objetivo de modernizar su infraestructura tecnológica y analítica, se desarrolló un proyecto integral de ingeniería de datos que permitió:
• Poblar una base de datos transaccional PostgreSQL con información sintética representativa del negocio.
• Diseñar y ejecutar consultas SQL complejas orientadas a resolver problemáticas operativas reales.
• Construir un pipeline ETL automatizado para migrar datos desde PostgreSQL hacia un modelo dimensional OLAP en Snowflake.

----
## Objetivos
Diseñar e implementar una solución moderna de gestión y análisis de datos para FleetLogix, integrando procesos transaccionales, analíticos y cloud computing.
Construir y poblar una base de datos relacional en PostgreSQL.
• Garantizar integridad referencial entre entidades del negocio.
• Generar datos sintéticos masivos coherentes con operaciones logísticas reales.
• Desarrollar consultas SQL avanzadas para análisis operacional.
• Implementar un modelo dimensional en Snowflake.
• Automatizar procesos ETL utilizando Python.

---
### Arquitectura de la solución

La solución completa se divide en tres grandes componentes:

---
## 1. Base de Datos Transaccional – PostgreSQL (OLTP)
Se utilizó PostgreSQL como sistema transaccional principal para almacenar información operativa relacionada con: Entregas, Viajes, Vehículos, Conductores, Clientes y Rutas.
El modelo relacional permitió representar las operaciones diarias de FleetLogix respetando las relaciones del negocio y garantizando integridad referencial.
Las principales tablas implementadas son deliveries, trips, vehicles, drivers, routes y customers
Generación de Datos Sintéticos
Se desarrollaron scripts para poblar la base de datos con información sintética masiva representativa de operaciones logísticas reales.
Las Características de los datos generados incluyen los siguientes parámetros: Distribución de entregas por ciudad, Conductores asociados a vehículos, Rutas urbanas e interurbanas, Estados de entrega, Consumo de combustible, Tiempos de viaje, Información histórica de entregas.
Los datos fueron generados respetando: Integridad referencial, Cardinalidad entre tablas, Restricciones de negocio y Consistencia temporal.

---
## 2. Consultas SQL y Análisis Operacional
Se desarrollaron consultas complejas orientadas a resolver problemáticas reales del negocio.
Algunos de los análisis implementados incluyen la determinación de conductores con mejor desempeño, vehículos con mayor consumo de combustible, rutas con mayor retraso, entregas fuera de tiempo, eficiencia logística por ciudad, indicadores de productividad, cálculo de costos operativos, tendencias de entregas.
Estas consultas permitieron transformar datos operacionales en información útil para la toma de decisiones.

---
## 3. Construcción del Modelo Dimensional en Snowflake
El objetivo principal es migrar la información operacional desde PostgreSQL hacia un entorno analítico OLAP optimizado para consultas de alto rendimiento.
Se diseñó un modelo dimensional tipo estrella compuesto por una tabla de hechos FACT_DELIVERIES y sus dimensiones asociadas DIM_DATE, DIM_TIME, DIM_DRIVER, DIM_VEHICLE, DIM_ROUTE y DIM_CUSTOMER.
La tabla de hechos contiene indicadores estratégicos como tiempo de entrega, retrasos, eficiencia de combustible, costos por entrega, ingresos estimados, productividad operativa, indicadores de puntualidad.

---
Desarrollo del Pipeline ETL
Para la elaboración del pipeline se utiliza Python como herramienta principal y librerías como Pandas, Psycopg2, Snowflake Connector, dotenv y logging
El script original utilizaba inserciones fila por fila mediante ciclos y múltiples sentencias MERGE, lo que generaba tiempos elevados de procesamiento, se realizó una optimización implementando write_pandas, tablas temporales de staging, operaciones merge masivas y procesamiento set-based.
Gracias a esta optimización se redujeron significativamente los tiempos de carga y se mejoró la escalabilidad del pipeline.

---
# Tecnologías Utilizadas
* PostgresSQL
* Snowflake
* Python : Pandas, Psycopg2,Snowflake Connector
---
# Conclusión
El proyecto FleetLogix permitió construir una solución integral de ingeniería de datos que combina procesamiento transaccional, modelado dimensional, automatización ETL. Asimismo, el uso de procesos automatizados y cargas optimizadas mejora significativamente la eficiencia del procesamiento de datos, permitiendo transformar información operacional en conocimiento estratégico para la toma de decisiones basada en datos.
