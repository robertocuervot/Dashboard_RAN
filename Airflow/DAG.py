CARPETA_ZIP = "/data/data/ftp/mae_evaluation/smart_capex"
CARPETA_DESCOMPRIMIDA = "/data/apps/repo-airflow/app_evotec/tmp"

from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 7, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
@dag(dag_id='ran_etl_pipeline', default_args=default_args, schedule='0 6 * * *', catchup=False)
def ran_etl_pipeline():
    from app_evotec.etl_scripts.Tasks_daily import descomprimir_archivos, borrar_encabezado, editar_archivos_csv, tablas_agregaciones

    @task
    def extract_task():
        descomprimir_archivos(CARPETA_ZIP, CARPETA_DESCOMPRIMIDA)
    @task
    def enhance_task_row():
        borrar_encabezado(CARPETA_DESCOMPRIMIDA)
    @task
    def enhance_task_column():
        editar_archivos_csv(CARPETA_DESCOMPRIMIDA)
    @task
    def load_task():
        tablas_agregaciones(CARPETA_DESCOMPRIMIDA)

    # Definir las dependencias entre las tareas
    extract_task() >> enhance_task_row() >> enhance_task_column() >> load_task()

# Instanciar el DAG
dag = ran_etl_pipeline()