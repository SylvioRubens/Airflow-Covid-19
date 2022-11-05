import pandas as pd
import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

from tabulate import tabulate
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

spark = None

default_args = {
    'owner': 'Sylvio',
    'dependes_on_past': False,
    'start_date': datetime(2022, 10, 6)
}


dag2 = DAG(
    'exercício_dag2',
    default_args = default_args,
    description = 'Segunda DAG do exercicio de arquitetura de dados, usada para a união dos indicadores',
    schedule_interval='@once', 
    catchup=False,
    tags = ['PUC', 'covid', 'trabalho']
)


def initializing_spark_session():
    spark = SparkSession \
    .builder \
    .appName("app") \
    .config("spark.driver.memory") \
    .getOrCreate()
    return spark

def read_spark(path):
    data = (
        spark
        .read
        .csv(path, header=True, sep=";", inferSchema=True)
    )
    return data

def create_view(data,view_name):
    data.createOrReplaceTempView(view_name)

def uniao_indicadores(**context):

    ti = context['ti']
    spark = ti.xcom_pull(task_ids='iniciar_sessao_spark')

    deaths_per_country = '/tmp/deaths_per_country.csv'
    death_per_million = '/tmp/death_per_million_per_country.csv'
    excess_mortality = '/tmp/excess_mortality_per_million_per_country.csv'
    population_density = '/tmp/population_density_per_country.csv'
    new_vaccinations = '/tmp/new_vaccinations_per_country.csv'

    dpc = read_spark(deaths_per_country)
    dpm = read_spark(death_per_million)
    em = read_spark(excess_mortality)
    pd = read_spark(population_density)
    nv = read_spark(new_vaccinations)

    create_view(data=dpc, view_name=deaths_per_country)
    create_view(data=dpm, view_name=death_per_million)
    create_view(data=em, view_name=excess_mortality)
    create_view(data=pd, view_name=population_density)
    create_view(data=nv, view_name=new_vaccinations)

    tabela_indicadores = spark.sql("""
        select 
            *
        from deaths_per_country as dpc
        left join death_per_million as dpm on (dpc.location = dpm.location)
        left join excess_mortality as em on (dpc.location = em.location)
        left join population_density as pd on (dpc.location = pd.location)
        left join new_vaccinations as nv on (dpc.location = nv.location)

    """)

    print(tabela_indicadores)


initialize = PythonOperator(
    task_id = "iniciar_sessao_spark",
    python_callable = initializing_spark_session,
    dag=dag2
)

uniao = PythonOperator(
    task_id = "uniao_indicadores",
    python_callable = uniao_indicadores,
    provide_context=True,
    dag=dag2
)

fim = DummyOperator(
    task_id="fim_dag_2",
    dag=dag2
)

initialize >> uniao >> fim