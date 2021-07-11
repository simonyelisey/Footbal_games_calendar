#!/usr/bin/env python
# coding: utf-8

# In[28]:


import requests
import json
import plotly.graph_objects as go
import plotly.io as pio
from google.cloud import bigquery
import pandas as pd
from fpdf import FPDF
import datetime as dt
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator


# In[74]:


def get_home_team_name(game):
  """
  return: название домашней команды
  """
  home_team = game['teams']['home']['name']


  return home_team


# In[75]:


def get_away_team_name(game):
  """
  return: нозвание гостевой команды
  """
  away_team = game['teams']['away']['name']


  return away_team


# In[76]:


def league_name(game):
  """
  return: название лиги
  """
  league = ', '.join(map(str, (game['league']['name'], game['league']['country'])))


  return league


# In[77]:


def stadium_name(game):
  """
  return: название стадиона
  """
  stadium = game['fixture']['venue']['name']


  return stadium


# In[78]:


def venue_name(game):
  """
  return: название города, в котором проходит матч
  """
  venue = game['fixture']['venue']['city']


  return venue


# In[79]:


def date_of_game(game):
    """
    return: дата матча
    """
    date = game['fixture']['date'].replace('T', ' ')

    return date


# In[80]:


def create_game_df(league, home_team, away_team, stadium, venue, date):
    """
    return: датафрейм со значениями из поданых данных
    """
    df = pd.DataFrame(
        {
            'League': [league],
            'Home_team': [home_team],
            'Away_team': [away_team],
            'Stadium': [stadium],
            'Venue': [venue],
            'Date': [date],
        })

    return df


# In[81]:


# 1

def download_games():
    ''''
    return: следующие 3 игры с нынешней даты
    '''
    # укажем url
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

    # установим параметр, ограничивающий загрузку 3 следующих матчей с данного момента
    querystring = {"next": "3"}

    # ключ и хост api
    headers = {
        'x-rapidapi-key': "ce76696982msh06564f175b8e48bp1ab3bcjsn94be55ded51f",
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    # определим словарь из загруженных игр
    games = dict(response.json())
    
    with open('/Users/simonyelisey/Parimatch/games_json/games_{}'.format(dt.date.today()), 'w') as f:
        json.dump({'game_1': games['response'][0],
                   'game_2': games['response'][1],
                   'game_3': games['response'][2]}, f)


# In[82]:


# 2

def create_all_games_df():
    '''
    создает датафрейм из 3 игр
    '''
    with open('/Users/simonyelisey/Parimatch/games_json/games_{}'.format(dt.date.today())) as f:
        games = json.load(f)
        game_1 = games['game_1']
        game_2 = games['game_2']
        game_3 = games['game_3']
    
    # определим нужные статистики
    home_team_1 = get_home_team_name(game_1)
    away_team_1 = get_away_team_name(game_1)
    home_team_2 = get_home_team_name(game_2)
    away_team_2 = get_away_team_name(game_2)
    home_team_3 = get_home_team_name(game_3)
    away_team_3 = get_away_team_name(game_3)

    league_1 = league_name(game_1)
    league_2 = league_name(game_2)
    league_3 = league_name(game_3)

    stadium_1 = stadium_name(game_1)
    stadium_2 = stadium_name(game_2)
    stadium_3 = stadium_name(game_3)

    venue_1 = venue_name(game_1)
    venue_2 = venue_name(game_2)
    venue_3 = venue_name(game_3)

    date_1 = date_of_game(game_1)
    date_2 = date_of_game(game_2)
    date_3 = date_of_game(game_3)
    
    # создадим датафрейм
    df = pd.concat([
        create_game_df(league_1, home_team_1, away_team_1, stadium_1, venue_1, date_1),
        create_game_df(league_2, home_team_2, away_team_2, stadium_2, venue_2, date_2),
        create_game_df(league_3, home_team_3, away_team_3, stadium_3, venue_3, date_3)
        ], ignore_index=True)
    
    # сохраним датафрейм
    df.to_csv('/Users/simonyelisey/Parimatch/data_frames/games_df_{}'.format(dt.date.today()))


# In[83]:


client_conf = '/Users/simonyelisey/Parimatch/virtual-anchor-319215-66b007559881.json' # конфигурации клиента на BigQuery
bq_client = bigquery.Client.from_service_account_json(client_conf) # адрес сервисного клиента

bq_table_id = 'virtual-anchor-319215.next_3_soccer_games.soccer_games' # определяем таблицу из BigQuery


# In[84]:


# 3

def send_df_to_bigquery():
    '''
    отправляет датафрейм на BigQuery
    '''
    df = pd.read_csv('/Users/simonyelisey/Parimatch/data_frames/games_df_{}'.format(dt.date.today()), index_col=0)
    client = bq_client
    table_id = bq_table_id

    result = client.load_table_from_dataframe(df, table_id).result()  # загружаем датафрейм на BigQuery


# In[85]:


# 4

def create_plotly_tables_pdf():
    """
    создает из датафрейма таблицы plotly, созраняет их в формате .jpeg,
    затем создает из них файл .pdf и сохраняет его
    """
    # определим датафрейм
    df = pd.read_csv('/Users/simonyelisey/Parimatch/data_frames/games_df_{}'.format(dt.date.today()), index_col=0)
    
    # создадим отдельную таблицу plotly на каждый матч 
    values_1 = df.iloc[0, :].transpose().values.tolist()
    fig_1 = go.Figure(data=[go.Table(header=dict(values=list(df.columns)),
                                   cells=dict(values=values_1))
                          ])
    
    values_2 = df.iloc[1, :].transpose().values.tolist()
    fig_2 = go.Figure(data=[go.Table(header=dict(values=list(df.columns)),
                                   cells=dict(values=values_2))
                          ])
    
    values_3 = df.iloc[2, :].transpose().values.tolist()
    fig_3 = go.Figure(data=[go.Table(header=dict(values=list(df.columns)),
                                   cells=dict(values=values_3))
                          ])
    
    # сохраним таблицы в формате.jpeg
    pio.write_image(fig_1,'/Users/simonyelisey/Parimatch/fig_images/fig_1_{}.jpeg'.format(dt.date.today()), width=900, height=400)
    pio.write_image(fig_2,'/Users/simonyelisey/Parimatch/fig_images/fig_2_{}.jpeg'.format(dt.date.today()), width=900, height=400)
    pio.write_image(fig_3,'/Users/simonyelisey/Parimatch/fig_images/fig_3_{}.jpeg'.format(dt.date.today()), width=900, height=400)
    
    pdf = FPDF()
    
    # создадим файл pdf 
    pdf.add_page()
    pdf.image('/Users/simonyelisey/Parimatch/fig_images/fig_1_{}.jpeg'.format(dt.date.today()), 15, 15, 175)
    pdf.image('/Users/simonyelisey/Parimatch/fig_images/fig_2_{}.jpeg'.format(dt.date.today()), 15, 57.5, 175)
    pdf.image('/Users/simonyelisey/Parimatch/fig_images/fig_3_{}.jpeg'.format(dt.date.today()), 15, 100, 175)
    
    # сохраним файл pdf
    pdf.output('/Users/simonyelisey/Parimatch/pdfs/games_{}.pdf'.format(dt.date.today()), 'F')


# In[ ]:


# далее настроим DAG для автоматического выполнения кода раз в день на Apache Airflow


# In[92]:


# зададим аргументы DAGа
args = {
    'owner': 'simonyelisey1',
    'start_date': dt.datetime(2021, 7, 10, 9),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}


# In[93]:


# определим DAG
with DAG(dag_id='football_games', default_args=args, schedule_interval='@daily') as dag:
    download_new_games = PythonOperator(
    task_id='download_games',
    python_callable=download_games, 
    dag=dag
    )
    games_df = PythonOperator(
    task_id='create_all_games_df',
    python_callable=create_all_games_df, 
    dag=dag
    )
    export_to_bigquery = PythonOperator(
    task_id='send_df_to_bigquery',
    python_callable=send_df_to_bigquery, 
    dag=dag
    )
    create_pdf = PythonOperator(
    task_id='create_plotly_tables_pdf',
    python_callable=create_plotly_tables_pdf, 
    dag=dag
    )
    download_new_games >> games_df >> export_to_bigquery >> create_pdf


# In[ ]:




