from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import hashlib
import os

# Configuración
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/gcs/data/credentials/etl-sri-ecuador-jose-bonilla-1b4d381e9dab.json'

# Variables
PROJECT_ID = "etl-sri-ecuador-jose-bonilla"   # tu proyecto
DATASET_ID = "modelo_sri_jb"                    # tu dataset
BUCKET_NAME = "datos_compras_ventas_sri_jb"     # bucket donde está tu CSV

# Función para descargar el CSV desde GCS
def descargar_csv(blob_name, local_path):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)

def cargar_datos():
    # Descarga y lee CSV
    blob_name = 'sri_ventas_2022.csv'  # ajusta si quieres otra
    local_path = '/tmp/sri_ventas_2022.csv'
    descargar_csv(blob_name, local_path)
    df = pd.read_csv(local_path, encoding='ISO-8859-1')
    
    # Crear id_tiempo
    df['id_tiempo'] = df.apply(lambda row: hashlib.md5(f"{row['AÑO']}{row['MES']}".encode()).hexdigest(), axis=1)

    # Renombrar columnas (ajusta a tus nombres)
    df.rename(columns={
        'CODIGO_SECTOR_N1':'codigo_sector_n1',
        'PROVINCIA':'codigo_provincia',
        'CANTON':'codigo_canton',
        'VENTAS_NETAS_TARIFA_GRAVADA':'ventas_tarifa_gravada',
        'VENTAS_NETAS_TARIFA_0':'ventas_tarifa_0',
        'VENTAS_NETAS_TARIFA_VARIABLE':'ventas_tarifa_variable',
        'VENTAS_NETAS_TARIFA_5':'ventas_tarifa_5',
        'EXPORTACIONES':'exportaciones',
        'COMPRAS_NETAS_TARIFA_GRAVADA':'compras_tarifa_gravada',
        'COMPRAS_NETAS_TARIFA_0':'compras_tarifa_0',
        'IMPORTACIONES':'importaciones',
        'COMPRAS_RISE':'compras_rise',
        'TOTAL_COMPRAS':'total_compras',
        'TOTAL_VENTAS':'total_ventas'
    }, inplace=True)

    # Convertir a float
    for col in ['ventas_tarifa_gravada','ventas_tarifa_0','ventas_tarifa_variable',
                'ventas_tarifa_5','exportaciones','compras_tarifa_gravada',
                'compras_tarifa_0','importaciones','compras_rise',
                'total_compras','total_ventas']:
        df[col] = df[col].astype(float)

    # Cargar en BigQuery (ajusta nombre completo)
    from google.cloud import bigquery
    client = bigquery.Client()
    client.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{DATASET_ID}.Hechos_VentasCompras"
    ).result()

def cargar_dim_tiempo():
    import pandas as pd
    import hashlib
    from google.cloud import bigquery

    # Ejemplo: generando datos ficticios
    df = pd.DataFrame({
        'AÑO': [2022, 2022],
        'MES': ['Enero', 'Febrero']
    })
    df['id_tiempo'] = df.apply(lambda row: hashlib.md5(f"{row['AÑO']}{row['MES']}".encode()).hexdigest(), axis=1)
    df['anio'] = df['AÑO'].astype(int)
    df['mes'] = df['MES']
    df['nombre_mes'] = df['MES']
    client = bigquery.Client()
    client.load_table_from_dataframe(
        df[['id_tiempo','anio','mes','nombre_mes']],
        f"{PROJECT_ID}.{DATASET_ID}.Dim_Tiempo"
    ).result()

# Define el DAG
default_args = {
    'owner': 'pancho',
    'start_date': datetime(2023,1,1),
    'retries': 1,
}
with DAG('etl_sri', 'ETL desde CSV en GCS a BigQuery', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    start = EmptyOperator(task_id='start')
    descarga_y_carga = PythonOperator(
        task_id='descargar_y_cargar',
        python_callable=cargar_datos
    )
    cargar_tiempo = PythonOperator(
        task_id='cargar_dim_tiempo',
        python_callable=cargar_dim_tiempo
    )
    end = EmptyOperator(task_id='end')

    # Dependencias
    start >> [descarga_y_carga, cargar_tiempo] >> end