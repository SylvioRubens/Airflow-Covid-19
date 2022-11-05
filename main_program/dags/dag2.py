import pandas as pd
import numpy as np

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

def read_data(path):
    data = pd.read_csv(path, sep=";")
    return data

def uniao_indicadores():

    deaths_per_country = '/tmp/deaths_per_country.csv'
    death_per_million = '/tmp/death_per_million_per_country.csv'
    excess_mortality = '/tmp/excess_mortality_per_million_per_country.csv'
    population_density = '/tmp/population_density_per_country.csv'
    new_vaccinations = '/tmp/new_vaccinations_per_country.csv'

    dpc = read_data(deaths_per_country)
    dpm = read_data(death_per_million)
    em = read_data(excess_mortality)
    pd = read_data(population_density)
    nv = read_data(new_vaccinations)

    df_merged = dpc.set_index(['continent', 'location']).join(dpm.set_index(['continent', 'location']), how='left') \
        .join(em.set_index(['continent', 'location']), how='left') \
        .join(pd.set_index(['continent', 'location']), how='left') \
        .join(nv.set_index(['continent', 'location']), how='left') \

    df_merged = df_merged.reset_index(names=['continent', 'location'])

    print(tabulate(df_merged, headers="keys", tablefmt="psql"))

    # Exportar a tabela de indicadores:
    tabela_indicadores_path = '/tmp/tabela_indicadores_path.csv'
    df_merged.to_csv(tabela_indicadores_path, index=False, sep=";")

initialize = DummyOperator(
    task_id = "inicio_dag_2",
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