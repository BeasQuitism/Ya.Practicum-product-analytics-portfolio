# filename=bookmate_dag.py

from datetime import datetime
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.yandex.operators.dataproc import DataprocCreatePysparkJobOperator


class PysparkJobOperator(DataprocCreatePysparkJobOperator):
    template_fields = ("cluster_id", "args",)


DAG_ID = "audition_content_analysis"

with DAG(
    dag_id=DAG_ID,
    schedule="0 16 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bookmate"],
) as dag:

    # 1) Ждём появления входного файла в S3
    wait_for_input = S3KeySensor(
        task_id="wait_for_input",
        poke_interval=300,
        timeout=3600,
        bucket_name="da-plus-dags",
        bucket_key="script_bookmate/data_{{ ds.replace('-', '_') }}/audition_content.csv",
        mode="poke",
        aws_conn_id="s3",
        wildcard_match=False,
    )

    # 2) Запускаем PySpark-задание на кластере Dataproc
    run_pyspark = PysparkJobOperator(
        name="create_bookmate_aggregate",
        task_id="run_pyspark",
        cluster_id="c9q4134h5vi546h1e148",
        args=["{{ ds }}"],
        main_python_file_uri="s3a://da-plus-dags/DATA_DELETED/jobs/my_spark_job.py"
    )

    # 3) Зависимости
    wait_for_input >> run_pyspark
