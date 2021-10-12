# Libraries

import json
import urllib.request
import pandas as pd
import datetime
import mysql.connector
from mysql.connector import connect, Error
import csv
import os
from dotenv import load_dotenv
load_dotenv()

# Params 

TOKEN = os.environ["TOKEN"]

sql_user = os.environ["USER"]
sql_password = os.environ["PASS"]
sql_db = os.environ["DB"]
sql_host = os.environ["HOST"]

# Timeseries range
startdate = "2017-01-01" 
enddate = "2018-12-31"

# Functions

def _get_headers(token):
    headers = dict()
    headers['Accept'] = 'application/json; application/vnd.esios-api-v1+json'
    headers['Content-Type'] = 'application/json'
    headers['Host'] = 'api.esios.ree.es'
    headers['Authorization'] = 'Token token=\"' + token + '\"'
    headers['Cookie'] = ''
    return headers

def _get_id(*args):
    # Perform the call
    url = 'https://api.esios.ree.es/indicators'
    req = urllib.request.Request(url, headers=_get_headers(TOKEN))
    with urllib.request.urlopen(req) as response:
        try:
            json_data = response.read().decode('utf-8')
        except:
            json_data = response.readall().decode('utf-8')

    result = json.loads(json_data)['indicators']

    _indicators_list = list()

    for entry in result:
        name = entry['name']
        id_ = entry['id']
        _indicators_list.append([name, id_])

        if name in 'Previsión diaria de la demanda eléctrica peninsular':
            n, _id = name, id_
    return _id

def demand_timeserie(startdate, enddate, _id):
    uri = f"https://api.esios.ree.es/indicators/{_id}?start_date={startdate}T00:00:00+02:00&end_date={enddate}T23:50:00+02:00&geo_agg=sum&geo_ids&time_trunc=hour&time_agg=&locale=es"

    req = urllib.request.Request(uri, headers=_get_headers(TOKEN))
    with urllib.request.urlopen(req) as response:
        try:
            json_data = response.read().decode('utf-8')
        except:
            json_data = response.readall().decode('utf-8')

    result = json.loads(json_data)
    return pd.json_normalize(result)

def normalize_df(df):
    try:
        if df.columns in ['indicator.name','indicator.id','indicator.values_updated_at','indicator.values']:
            df = df[['indicator.name','indicator.id','indicator.values_updated_at','indicator.values']]
        else:
            print("Something went wrong, please, check the DF downloaded")
        
        df = pd.concat([df.explode('indicator.values').drop(['indicator.values'], axis=1),
        df.explode('indicator.values')['indicator.values'].apply(pd.Series)], axis=1)
        df = df.reset_index(drop=True)

        _cols = {
            'indicator.name': 'name', 
            'indicator.id': 'id', 
            'indicator.values_updated_at':'update_timestamp',
            'value': 'demand_forecast', 
        }
        df = df.rename(columns=_cols)
        df = df[['name','id','datetime','demand_forecast','update_timestamp']]

        df['datetime'] = df['datetime'].apply(lambda x: pd.Timestamp(x[0:-7])).dt.tz_localize('Europe/Madrid', ambiguous=True)
        df['update_timestamp'] = df['update_timestamp'].apply(lambda x: pd.Timestamp(x[0:-7])).dt.tz_localize('Europe/Madrid', ambiguous=True)

        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['update_timestamp'] = pd.to_datetime(df['update_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        print('There has been issues with the transformation process')
    return df

def test_db_connection(_host,_user,_password,_db):
    try:
        with connect(
            host=_host,
            user=_user,
            password=_password,
            database=_db
        ) as connection:
            return connection
    except Error as e:
        return e

def insert_in_table(_data):
    connection = connect(sql_host, sql_user, sql_password, sql_db)
    query = """INSERT INTO demand_forecast(name, id, datetime, demand_forecast, update_timestamp) VALUES (%s,%s,%s,%s,%s)"""

    with connection:
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print("Database version: {}".format(version[0]))

    conn = None
    try:
        conn = connect(sql_host, sql_user, sql_password, sql_db)
        cur = conn.cursor()
        for row in _data:
            cur.execute(query, row)
        cur.close()
        conn.commit()
    except (Exception, mysql.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



# Execution

demand_id = _get_id()
print('We suscessfully obtained the Previsión diaria indicator id')

raw_df = demand_timeserie(startdate,enddate,demand_id)
print('We suscessfully downloaded the data. Now, we proceed to transform it to desired format')

transformed_df = normalize_df(raw_df)
transformed_df.to_csv('demand_forecast.csv')
print('CSV ready to be uploaded')

print('Testing DB connection')
test_db_connection(sql_host,sql_user,sql_password,sql_db)

_data = csv.reader(file('demand_forecast.csv'))
insert_in_table(_data)
print('ETL finished. Proceed to query the table to check that all data is OK')