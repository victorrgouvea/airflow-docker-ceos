
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_utils.python_callables.raw_csv_to_parquet import process_csv_to_parquet
from airflow_utils.python_callables.parquet_to_iceberg import process_parquet_to_iceberg
from airflow_utils.schemas.raw_schemas import sfinge_notas_fiscais_schema

with DAG(
    'parquet_to_iceberg_dag',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'on_failure_callback': some_function, #gerar mensagem slack
    },
    description='Sends parquet data to iceberg',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    Sfinge__notas_fiscais__raw_to_parquet = PythonOperator(
        task_id="Sfinge__notas_fiscais__raw_to_parquet",
        python_callable=process_csv_to_parquet,
        op_kwargs = {
                     "path":"Sfinge__notas_fiscais_exemplo/notas_fiscais",
                     "filename": "202401_NFe_NotaFiscal",
                     "schema": sfinge_notas_fiscais_schema,
                     "date_columns":{
                         "DATA EMISSÃO":"dd/mm/yyyy"
                     },
                     "timestamp_columns":{
                         "DATA/HORA EVENTO MAIS RECENTE":"dd/mm/yyyy hh:mm:ss"
                     },
                     "number_columns":["VALOR NOTA FISCAL"],
                    },
    )

    Sfinge__notas_fiscais__parquet_to_iceberg = PythonOperator(
        task_id="Sfinge__notas_fiscais__parquet_to_iceberg",
        python_callable=process_parquet_to_iceberg,
        op_kwargs = {
                     "path":"Sfinge__notas_fiscais_exemplo/notas_fiscais",
                     "filename": "202401_NFe_NotaFiscal",
                     "table_name": "notas_fiscais_teste"
        }
    )
    
    Sfinge__notas_fiscais__raw_to_parquet >> Sfinge__notas_fiscais__parquet_to_iceberg

    # t2 = BashOperator(
    #     task_id='sleep',
    #     depends_on_past=False,
    #     bash_command='sleep 5',
    #     retries=3,
    # )
    # t1.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    # """
    # )

    # dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # otherwise, type it like this
    # templated_command = dedent(
    #     """
    # {% for i in range(5) %}
    #     echo "{{ ds }}"
    #     echo "{{ macros.ds_add(ds, 7)}}"
    # {% endfor %}
    # """
    # )

    # t3 = BashOperator(
    #     task_id='templated',
    #     depends_on_past=False,
    #     bash_command=templated_command,
    # )

    # t1 >> [t2, t3]