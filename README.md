# Footbal_games_calendar
next three games definition Python script

В данном проекте я реализовал скрипт **Python**, который:

- загружает через https://rapidapi.com/api-sports/api/api-football/ данные о трех следующих футбольных матчах с даты выгрузки;
- формирует из этих матчей дата фрейм и отправляет его на сервис **BigQuery**;
- формирует на каждый матч отдельную таблицу **Plotly**;
- создает из получившихся таблиц файл PDF, и сохраняте его на локальную память.

Данный скрипт выполняется автоматически один раз в день. Автоматизацию я совершил при помощи **Apache Airflow**.

Схема выполнения DAGa в Airflow:
<img width="1129" alt="Снимок экрана 2021-07-18 в 23 59 46" src="https://user-images.githubusercontent.com/65309131/126081974-0dc0413f-e326-42fa-863b-0eedb74008c7.png">


Полезные ссылки: 
- https://www.applydatascience.com/airflow/writing-your-first-pipeline/
- https://cloud.google.com/bigquery/docs/tutorials
- http://datalytics.ru/all/kak-ispolzovat-google-bigquery-s-pomoschyu-python/
