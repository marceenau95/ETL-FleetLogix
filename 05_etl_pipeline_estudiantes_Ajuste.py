"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import logging
import schedule
import time
import json
from typing import Dict, List
import os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas #Ajuste importar Conector Snowflake y Pandas

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
load_dotenv('credenciales.env')
host = os.getenv('PG_HOST')
database_pg = os.getenv('PG_DATABASE')
user_pg = os.getenv('PG_USER')
password_pg = os.getenv('PG_PASSWORD')
port = int(os.getenv('PG_PORT', 5432))

user= os.getenv('SF_USER')
password=os.getenv('SF_PASSWORD')
account= os.getenv('SF_ACCOUNT')
warehouse=os.getenv('SF_WAREHOUSE')
database=os.getenv('SF_DATABASE')
schema=os.getenv('SF_SCHEMA')

POSTGRES_CONFIG = {
    'host': host,
    'database': database_pg,
    'user': user_pg,
    'password': password_pg,
    'port': port
}

SNOWFLAKE_CONFIG = {
     'user': user,
    'password': password,
    'account': account,
    'warehouse': warehouse,
    'database': database,
    'schema': schema,
    'autocommit': False  # FIX: control explícito de transacciones
}


class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }

    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info("Conectado a PostgreSQL")

            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info("Conectado a Snowflake")

            return True
        except Exception as e:
            logging.error(f"Error en conexión: {e}")
            return False

    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info("Iniciando extracción de datos...")

        query = """
        SELECT  d.delivery_id,
                    d.customer_name,
                    d.tracking_number,
                    d.package_weight_kg,
                    d.scheduled_datetime,
                    d.delivered_datetime,
                    d.recipient_signature,
                    d.delivery_status,
                    t.trip_id,
                    t.departure_datetime,
                    t.arrival_datetime,
                    t.fuel_consumed_liters,
                    t.vehicle_id,
                    t.driver_id,
                    t.route_id,
                    r.origin_city,
                    r.destination_city,
                    r.distance_km,
                    r.toll_cost,
                    dr.first_name,
                    dr.last_name,
                    v.acquisition_date,
                    dr.hire_date,
                    v.license_plate,
                    v.vehicle_type,
                    v.capacity_kg,
                    v.fuel_type,
                    v.status as vehicle_status,
                    dr.employee_code,
                    dr.license_number,
                    dr.license_expiry,
                    dr.phone,
                    dr.status as driver_status,
                    r.route_code,
                    r.estimated_duration_hours
            FROM deliveries d
            JOIN trips t   ON d.trip_id = t.trip_id
            JOIN routes r  ON r.route_id = t.route_id
            JOIN vehicles v on v.vehicle_id=t.vehicle_id
            LEFT JOIN drivers dr ON dr.driver_id = t.driver_id
             where d.delivered_datetime >= ( current_date - INTERVAL '90
               days')
        """

        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f"Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f"Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info("Iniciando transformación de datos...")

        try:
            # Parse de datetimes
            for col in ['scheduled_datetime', 'delivered_datetime',
                        'departure_datetime', 'arrival_datetime']:
                df[col] = pd.to_datetime(df[col])

            # Descartar filas sin fecha de entrega real
            df = df.dropna(subset=['delivered_datetime', 'scheduled_datetime'])

            # Tiempo total desde programado hasta entregado
            df['delivery_time_minutes'] = (
                (df['delivered_datetime'] - df['scheduled_datetime'])
                .dt.total_seconds() / 60
            ).round(2)

            # Retraso: positivo = tarde; negativo (adelantado) se recorta a 0
            df['delay_minutes'] = df['delivery_time_minutes'].clip(lower=0).round(2)
            df['is_on_time'] = df['delay_minutes'] <= 30

            # FIX: eliminado el filtro que descartaba entregas adelantadas
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]

            # Duración del viaje en horas
            df['trip_duration_hours'] = (
                (df['arrival_datetime'] - df['departure_datetime'])
                .dt.total_seconds() / 3600
            ).round(2)

            # FIX: evitar división por cero reemplazando 0 con NaN
            df['trip_duration_hours'] = df['trip_duration_hours'].replace(0, np.nan)
            df['fuel_consumed_liters'] = df['fuel_consumed_liters'].replace(0, np.nan)

            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)

            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)

            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)

            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'].fillna(0) * 5000 + df['toll_cost']) /
                df['deliveries_in_trip']
            ).round(2)

            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)

            # FIX #6: has_signature debe ser BOOLEAN, no el texto de la firma
            df['has_signature'] = (
                df['recipient_signature'].notna() & (df['recipient_signature'] != '')
            )

            # Campos SCD Type 2
            df['valid_from'] = df['scheduled_datetime'].dt.date
            df['valid_to'] = date(2099, 12, 31)
            df['is_current'] = True

            # Nombre completo del conductor
            df['full_name'] = df['first_name'].str.strip() + ' ' + df['last_name'].str.strip()

            # Antigüedad en meses (vehículo y conductor)
            today = date.today()
            df['age_months'] = df['acquisition_date'].apply(
                lambda x: (today.year - x.year) * 12 + (today.month - x.month)
                if pd.notna(x) else None
            )
            df['experience_months'] = df['hire_date'].apply(
                lambda x: (today.year - x.year) * 12 + (today.month - x.month)
                if pd.notna(x) else None
            )

            self.metrics['records_transformed'] = len(df)
            logging.info(f"Transformados {len(df)} registros")
            return df

        except Exception as e:
            logging.error(f"Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # FIX #4: Métodos auxiliares para poblar dim_date y dim_time
    # -------------------------------------------------------------------------

    def _ensure_dim_date(self, cursor, unique_dates):
        """Insertar fechas en dim_date si todavía no existen."""
        for d in unique_dates:
            if pd.isna(d):
                continue
            # Aceptar tanto date como Timestamp
            if hasattr(d, 'date'):
                d = d.date()
            date_key = int(d.strftime('%Y%m%d'))
            day_of_week = d.isoweekday()   # 1=Lunes … 7=Domingo
            is_weekend = day_of_week >= 6
            quarter = (d.month - 1) // 3 + 1

            cursor.execute("""
                MERGE INTO dim_date t
                USING (SELECT %s AS date_key) s ON t.date_key = s.date_key
                WHEN NOT MATCHED THEN INSERT (
                    date_key, full_date, day_of_week, day_name, day_of_month,
                    day_of_year, week_of_year, month_num, month_name,
                    quarter, year, is_weekend, is_holiday, holiday_name,
                    fiscal_quarter, fiscal_year
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, FALSE, NULL, %s, %s
                )
            """, (
                date_key,
                date_key, d, day_of_week, d.strftime('%A'),
                d.day, d.timetuple().tm_yday, d.isocalendar()[1],
                d.month, d.strftime('%B'),
                quarter, d.year, is_weekend,
                quarter, d.year
            ))

    def _ensure_dim_time(self, cursor, time_keys):
        """Insertar registros en dim_time (agrupados por hora) si no existen."""
        def time_of_day(h):
            if h < 6:
                return 'Madrugada'
            elif h < 12:
                return 'Mañana'
            elif h < 18:
                return 'Tarde'
            return 'Noche'

        for tk in set(time_keys):
            if pd.isna(tk):
                continue
            hour = int(tk) // 100
            is_business = 8 <= hour < 18
            if 6 <= hour < 14:
                shift = 'Turno 1'
            elif 14 <= hour < 22:
                shift = 'Turno 2'
            else:
                shift = 'Turno 3'
            hour_12 = (hour % 12) or 12
            am_pm = 'AM' if hour < 12 else 'PM'

            cursor.execute("""
                MERGE INTO dim_time t
                USING (SELECT %s AS time_key) s ON t.time_key = s.time_key
                WHEN NOT MATCHED THEN INSERT (
                    time_key, hour, minute, second, time_of_day,
                    hour_24, hour_12, am_pm, is_business_hour, shift
                ) VALUES (%s, %s, 0, 0, %s, %s, %s, %s, %s, %s)
            """, (
                tk,
                tk, hour, time_of_day(hour),
                f"{hour:02d}:00",
                f"{hour_12:02d}:00 {am_pm}",
                am_pm, is_business, shift
            ))

    # -------------------------------------------------------------------------
    # FIX #2 / #3 / #4 / #5: load_dimensions corregido 
    # Ajuste uso de Write Pandas en tablas temporales y operaciones MERGE set-based sobre
    # la tabla temporal 
    # -------------------------------------------------------------------------

    def load_dimensions(self, df: pd.DataFrame) -> Dict:
        """
        Cargar o actualizar todas las dimensiones en Snowflake.
        Retorna un dict con los mapeos {source_id -> surrogate_key} para
        que load_facts pueda resolver las claves correctamente.
        """
        logging.info("Cargando dimensiones...")
        cursor = self.sf_conn.cursor()
        key_maps: Dict = {}

        try:
            # --- dim_vehicle (SCD Type 2 simplificado) ---
            cursor.execute("""
            CREATE OR REPLACE TEMP TABLE TEMP_VEHICLE (
                VEHICLE_ID INT,
                LICENSE_PLATE STRING,
                VEHICLE_TYPE STRING,
                CAPACITY_KG FLOAT,
                FUEL_TYPE STRING,
                ACQUISITION_DATE DATE,
                AGE_MONTHS INT,
                VEHICLE_STATUS STRING
            )
            """)
            
            vehicles = df[['vehicle_id', 'license_plate', 'vehicle_type', 'capacity_kg',
                           'fuel_type', 'acquisition_date', 'age_months',
                           'vehicle_status']].drop_duplicates('vehicle_id')
            vehicles.columns = [
                'VEHICLE_ID','LICENSE_PLATE','VEHICLE_TYPE',
                'CAPACITY_KG','FUEL_TYPE','ACQUISITION_DATE',
                'AGE_MONTHS','VEHICLE_STATUS'
            ]
            
            write_pandas(self.sf_conn, vehicles, 'TEMP_VEHICLE')
            
            cursor.execute("""
            MERGE INTO dim_vehicle AS t
            USING TEMP_VEHICLE AS s
            ON t.vehicle_id = s.VEHICLE_ID
            AND t.is_current = TRUE

            WHEN NOT MATCHED THEN
            INSERT (
                vehicle_key,
                vehicle_id,
                license_plate,
                vehicle_type,
                capacity_kg,
                fuel_type,
                acquisition_date,
                age_months,
                status,
                valid_from,
                valid_to,
                is_current
            )
            VALUES (
                s.VEHICLE_ID,
                s.VEHICLE_ID,
                s.LICENSE_PLATE,
                s.VEHICLE_TYPE,
                s.CAPACITY_KG,
                s.FUEL_TYPE,
                s.ACQUISITION_DATE,
                s.AGE_MONTHS,
                s.VEHICLE_STATUS,
                CURRENT_DATE(),
                '2099-12-31',
                TRUE
             )
            """)
               
            key_maps['vehicle'] = dict(cursor.execute(
            "SELECT vehicle_id, vehicle_key FROM dim_vehicle WHERE is_current=TRUE"
        ).fetchall())

            # --- dim_driver (SCD Type 2 simplificado) ---
            cursor.execute("""
        CREATE OR REPLACE TEMP TABLE TEMP_DRIVER (
            DRIVER_ID INT,
            EMPLOYEE_CODE STRING,
            FULL_NAME STRING,
            LICENSE_NUMBER STRING,
            LICENSE_EXPIRY DATE,
            PHONE STRING,
            HIRE_DATE DATE,
            EXPERIENCE_MONTHS INT,
            DRIVER_STATUS STRING
        )
        """)
            
            drivers = df[['driver_id', 'employee_code', 'full_name', 'license_number',
                          'license_expiry', 'phone', 'hire_date', 'experience_months',
                          'driver_status']].drop_duplicates('driver_id')
            drivers.columns = [
            'DRIVER_ID','EMPLOYEE_CODE','FULL_NAME','LICENSE_NUMBER',
            'LICENSE_EXPIRY','PHONE','HIRE_DATE','EXPERIENCE_MONTHS','DRIVER_STATUS'
        ]
            
            write_pandas(self.sf_conn, drivers, 'TEMP_DRIVER')

            cursor.execute("""
            MERGE INTO dim_driver t
            USING TEMP_DRIVER s
            ON t.driver_id = s.driver_id AND t.is_current = TRUE
            WHEN NOT MATCHED THEN INSERT (
                driver_key, driver_id, employee_code, full_name,
                license_number, license_expiry, phone, hire_date,
                experience_months, status, performance_category,
                valid_from, valid_to, is_current
            )
            VALUES (
                s.DRIVER_ID, s.DRIVER_ID, s.EMPLOYEE_CODE, s.FULL_NAME,
                s.LICENSE_NUMBER, s.LICENSE_EXPIRY, s.PHONE, s.HIRE_DATE,
                s.EXPERIENCE_MONTHS, s.DRIVER_STATUS, 'Medio',
                CURRENT_DATE(), '2099-12-31', TRUE
            )
            """)

            key_maps['driver'] = dict(cursor.execute(
                """SELECT driver_id, driver_key FROM dim_driver WHERE is_current=TRUE"""
            ).fetchall())
            

            # --- dim_route ---

            cursor.execute("""
            CREATE OR REPLACE TEMP TABLE TEMP_ROUTE (
                ROUTE_ID INT,
                ROUTE_CODE STRING,
                ORIGIN_CITY STRING,
                DESTINATION_CITY STRING,
                DISTANCE_KM FLOAT,
                ESTIMATED_DURATION_HOURS FLOAT,
                TOLL_COST FLOAT
            )
            """)

            routes = df[['route_id', 'route_code', 'origin_city', 'destination_city',
                         'distance_km', 'estimated_duration_hours',
                         'toll_cost']].drop_duplicates('route_id')
            routes.columns = [
            'ROUTE_ID','ROUTE_CODE','ORIGIN_CITY','DESTINATION_CITY',
            'DISTANCE_KM','ESTIMATED_DURATION_HOURS','TOLL_COST'
            ]
            write_pandas(self.sf_conn, routes, 'TEMP_ROUTE')

            cursor.execute("""
                MERGE INTO dim_route t
                USING TEMP_ROUTE s
                ON t.route_id = s.route_id
                WHEN NOT MATCHED THEN INSERT (
                    route_key, route_id, route_code, origin_city,
                    destination_city, distance_km,
                    estimated_duration_hours, toll_cost,
                    difficulty_level, route_type
                )
                VALUES (
                    s.ROUTE_ID, s.ROUTE_ID, s.ROUTE_CODE, s.ORIGIN_CITY,
                    s.DESTINATION_CITY, s.DISTANCE_KM,
                    s.ESTIMATED_DURATION_HOURS, s.TOLL_COST,
                    'Medio', 'Interurbana'
                )
            """)
           
            cursor.execute("""
            SELECT route_id, route_key 
            FROM dim_route
            """)

            key_maps['route'] = dict(cursor.fetchall())

            # --- dim_customer ---
           
            cursor.execute("""
            CREATE OR REPLACE TEMP TABLE TEMP_CUSTOMER (
                CUSTOMER_NAME STRING,
                DESTINATION_CITY STRING
            )
            """)

            customers = df[['customer_name', 'destination_city']].drop_duplicates('customer_name')
            
            customers.columns = ['CUSTOMER_NAME','DESTINATION_CITY']
           
            write_pandas(self.sf_conn, customers, 'TEMP_CUSTOMER')

            cursor.execute("""
            MERGE INTO dim_customer t
            USING TEMP_CUSTOMER s
            ON t.customer_name = s.customer_name
            WHEN NOT MATCHED THEN INSERT (
                customer_key, customer_name, customer_type,
                city, first_delivery_date, total_deliveries, customer_category
            )
            VALUES (
                SEQ_CUSTOMER.NEXTVAL, s.CUSTOMER_NAME, 'Individual',
                s.DESTINATION_CITY, CURRENT_DATE(), 0, 'Regular'
            )
            """)

            key_maps['customer'] = dict(cursor.execute(
                """SELECT customer_name, customer_key FROM dim_customer"""
            ).fetchall())
        

            # --- dim_date y dim_time ---
            unique_dates = df['scheduled_datetime'].dt.date.unique()
            self._ensure_dim_date(cursor, unique_dates)

            sched_tks = (df['scheduled_datetime'].dt.hour * 100).unique().tolist()
            deliv_tks = (df['delivered_datetime'].dt.hour * 100).unique().tolist()
            self._ensure_dim_time(cursor, sched_tks + deliv_tks)

            self.sf_conn.commit()
            logging.info("Dimensiones actualizadas")
            return key_maps

        except Exception as e:
            logging.error(f"Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
            return {}
        # Ajuste uso de Write Pandas en tablas temporales y operaciones MERGE set-based sobre
            #la tabla temporal 
    def load_facts(self, df: pd.DataFrame, key_maps: Dict):
        logging.info("Cargando hechos (OPTIMIZADO)...")

        df['vehicle_key'] = df['vehicle_id'].map(key_maps['vehicle'])
        df['driver_key'] = df['driver_id'].map(key_maps['driver'])
        df['route_key'] = df['route_id'].map(key_maps['route'])
        df['customer_key'] = df['customer_name'].map(key_maps['customer'])

        df = df.dropna(subset=['vehicle_key','driver_key','route_key','customer_key'])

        df['date_key'] = df['scheduled_datetime'].dt.strftime('%Y%m%d').astype(int)
        df['scheduled_time_key'] = df['scheduled_datetime'].dt.hour * 100
        df['delivered_time_key'] = df['delivered_datetime'].dt.hour * 100

        df['etl_batch_id'] = self.batch_id

        fact_df = df[[
            'date_key','scheduled_time_key','delivered_time_key',
            'vehicle_key','driver_key','route_key','customer_key',
            'delivery_id','trip_id','tracking_number',
            'package_weight_kg','distance_km','fuel_consumed_liters',
            'delivery_time_minutes','delay_minutes',
            'deliveries_per_hour','fuel_efficiency_km_per_liter',
            'cost_per_delivery','revenue_per_delivery',
            'is_on_time','has_signature','delivery_status','etl_batch_id'
        ]]
        
        fact_df.columns = [c.upper() for c in fact_df.columns]
        
        write_pandas(self.sf_conn, fact_df, 'FACT_DELIVERIES')

        self.metrics['records_loaded'] = len(fact_df)
        logging.info(f"Hechos cargados: {len(fact_df)}")

    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f"Iniciando ETL - Batch ID: {self.batch_id}")

        try:
            if not self.connect_databases():
                return

            df = self.extract_daily_data()
            if df.empty:
                logging.warning("No hay registros para procesar.")
                self.close_connections()
                return

            df_transformed = self.transform_data(df)
            if df_transformed.empty:
                logging.warning("El DataFrame quedó vacío tras la transformación.")
                self.close_connections()
                return

            # load_dimensions retorna los mapeos; load_facts los consume
            key_maps = self.load_dimensions(df_transformed)
            if key_maps:
                self.load_facts(df_transformed, key_maps)

            self._calculate_daily_totals()
            self.close_connections()

            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f"ETL completado en {duration:.2f} segundos")
            logging.info(f"Métricas: {json.dumps(self.metrics, indent=2)}")

        except Exception as e:
            logging.error(f"Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()

    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()

        try:
            # Crear tabla de totales si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS TOTALES (
                    DATE_KEY INT,
                    NUM_ENTREGAS INT,
                    TOTAL_REVENUE DECIMAL(18,2),
                    AVG_DELIVERY_TIME DECIMAL(18,2),
                    ON_TIME_RATE DECIMAL(18,2),
                    ETL_BATCH_ID INT
                )
            """)
            cursor.execute("""
                SELECT COUNT(*)
                FROM FACT_DELIVERIES
                WHERE ETL_BATCH_ID = %s
                 """, (self.batch_id,))

            total_records = cursor.fetchone()[0]

            logging.info(f"Registros encontrados para batch {self.batch_id}: {total_records}")
            # Insertar totales del día
            cursor.execute("""
                INSERT INTO TOTALES (
            DATE_KEY,
            NUM_ENTREGAS,
            TOTAL_REVENUE,
            AVG_DELIVERY_TIME,
            ON_TIME_RATE,
            ETL_BATCH_ID
            )

            SELECT
            DATE_KEY,
            COUNT(*) AS NUM_ENTREGAS,
            SUM(REVENUE_PER_DELIVERY) AS TOTAL_REVENUE,
            ROUND(AVG(DELIVERY_TIME_MINUTES), 2) AS AVG_DELIVERY_TIME,
            ROUND(AVG(CASE WHEN IS_ON_TIME THEN 1 ELSE 0 END), 2) AS ON_TIME_RATE,
            ETL_BATCH_ID

            FROM FACT_DELIVERIES

            WHERE ETL_BATCH_ID = %s

            GROUP BY DATE_KEY, ETL_BATCH_ID
            """, (self.batch_id,))

            self.sf_conn.commit()
            logging.info("Totales diarios calculados")

        except Exception as e:
         logging.error(f"Error calculando totales: {e}")
         self.sf_conn.rollback()

    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info("Conexiones cerradas")


def job():
    etl = FleetLogixETL()
    etl.run_etl()


def main():
    """Función principal - Automatización diaria"""
    logging.info("Pipeline ETL FleetLogix iniciado")

    schedule.every().day.at("02:00").do(job)

    logging.info("ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")

    # Ejecutar una vez al inicio para pruebas
    job()

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
