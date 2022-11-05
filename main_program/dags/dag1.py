import pandas as pd
import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

from tabulate import tabulate
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

url_dataset = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
local_file = '/tmp/covid_dataset.csv'

default_args = {
    'owner': 'Sylvio',
    'dependes_on_past': False,
    'start_date': datetime(2022, 10, 6)
}

dag = DAG(
    'exercício_dag1',
    default_args = default_args,
    description = 'Primeira DAG do exercicio de arquitetura de dados',
    schedule_interval='@once', 
    catchup=False,
    tags = ['PUC', 'covid', 'trabalho']
)


def data_ingestion():
    dataset = pd.read_csv(url_dataset, sep=',')
    dataset.to_csv(local_file, index=False, sep=';')
    print("Dado do trabalho salvo!!!")


def calc_and_save_table(df, first_class, second_class, column_agg, new_column_name, operation, nome_tabela):
    res = df.groupby([first_class, second_class]).agg({column_agg: operation}).rename(columns = {column_agg: new_column_name}).reset_index()
    res.to_csv(nome_tabela, index=False, sep=";")
    print('\n', nome_tabela)
    print(tabulate(res, headers="keys", tablefmt="psql"))



def deaths_per_country():
    NOME_TABELA = '/tmp/deaths_per_country.csv'
    df = pd.read_csv(local_file, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='continent', 
        second_class='location', 
        column_agg='new_deaths',
        new_column_name='count_deaths',
        operation=np.sum,
        nome_tabela=NOME_TABELA)


def death_per_million_per_country():
    NOME_TABELA = '/tmp/death_per_million_per_country.csv'
    df = pd.read_csv(local_file, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='continent', 
        second_class='location', 
        column_agg='new_deaths_per_million',
        new_column_name='deaths_per_million',
        operation=np.sum,
        nome_tabela=NOME_TABELA)

def excess_mortality_per_million_per_country():

    NOME_TABELA = '/tmp/excess_mortality_per_million_per_country.csv'
    df = pd.read_csv(local_file, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='continent', 
        second_class='location', 
        column_agg='excess_mortality_cumulative_per_million',
        new_column_name='excess_mortality_cumulative_per_million',
        operation=np.max,
        nome_tabela=NOME_TABELA)

def population_density_per_country():

    NOME_TABELA = '/tmp/population_density_per_country.csv'
    df = pd.read_csv(local_file, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='continent', 
        second_class='location', 
        column_agg='population_density',
        new_column_name='population_density',
        operation=np.max,
        nome_tabela=NOME_TABELA)

def new_vaccinations_per_country():

    NOME_TABELA = '/tmp/new_vaccinations_per_country.csv'
    df = pd.read_csv(local_file, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='continent', 
        second_class='location', 
        column_agg='new_vaccinations',
        new_column_name='vaccinations_applied',
        operation=np.sum,
        nome_tabela=NOME_TABELA)

ingestion = PythonOperator(
    task_id = "data_ingestion",
    python_callable = data_ingestion,
    dag=dag
)

calc1 = PythonOperator(
    task_id = "deaths_per_country",
    python_callable = deaths_per_country,
    dag=dag
)
calc2 = PythonOperator(
    task_id = "death_per_million_per_country",
    python_callable = death_per_million_per_country,
    dag=dag
)
calc3 = PythonOperator(
    task_id = "excess_mortality_per_million_per_country",
    python_callable = excess_mortality_per_million_per_country,
    dag=dag
)
calc4 = PythonOperator(
    task_id = "population_density_per_country",
    python_callable = population_density_per_country,
    dag=dag
)
calc5 = PythonOperator(
    task_id = "new_vaccinations_per_country",
    python_callable = new_vaccinations_per_country,
    dag=dag
)
trigger_dag2 = TriggerDagRunOperator(
    task_id = 'trigger_dag2',
    trigger_dag_id = 'exercício_dag2',
    dag=dag
)


ingestion >> [calc1, calc2, calc3, calc4, calc5] >> trigger_dag2
