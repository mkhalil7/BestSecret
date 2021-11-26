# The goal of this class is to provide a pipeline to retireve historical exchange rate data
# The base currency as well as the start and end date can be configured as parameters


# the configatuin parametrs are present in the config.ini file
import pandas as pd
import requests
import configparser
import sqlite3
from sqlite3 import Error


# reading the config file
config = configparser.ConfigParser()
config.read('config.ini')


# pipeline constant parameters
base_currency = "EUR"
start_date = "2021-11-19"
end_date = "2021-11-26"


# api parameters
api_key = config.get('api', 'token')
api_url = config.get('api', 'url')


# SQlLite parameters
db_name = config.get('sqlLite', 'db_name')


# send an API request and add the data
def get_currency_api_data():

    if(len(api_key) == 0 or len(api_url) == 0):
        raise Exception(
            'Missing api URL or token please check the configuration file')

    params = {
        'access_key': api_key,
        'base': base_currency,
        'start_date': start_date,
        'end_date': end_date
    }

    # sending the request to the API
    response = requests.get(api_url, params=params)
    response_json = response.json()

    if(response.status_code == 200 and response_json['success']):

        # loading everything to data frame
        df = pd.DataFrame.from_dict(
        pd.json_normalize(response_json),  orient='columns')

        # removing unecessary columns
        del df['success']
        del df['timeseries']
        del df['start_date']
        del df['end_date']

        # using melt to unpivot table
        df = pd.melt(df, id_vars=["base"], var_name="temp", value_name="Value")
        df[['Rate', 'Date', 'Currency']] = df['temp'].str.split(
            '.', 2, expand=True)

        #renaming and reorganizing
        df.columns = df.columns.str.replace('base', 'base_code')
        df.columns = df.columns.str.replace('Date', 'date')
        df.columns = df.columns.str.replace('Currency', 'currency_code')
        df.columns = df.columns.str.replace('Value', 'rate')

        df = df[['base_code', 'date', 'rate', 'currency_code']]

        return df

    else:
        raise Exception(
            'API query failed with folowing error message :' + response_json['error']['info'])


# store the data into a sqlite database
def store_date_sqlite(df):

    conn = None
    try:
        conn = sqlite3.connect(db_name)
    except Error as e:
        print(e)

    df.to_sql('currency_conversion', conn, if_exists='append', index=False)
    return conn


def get_currency_average(currency, date_start, date_end, conn):
    cur = conn.cursor()
    cur.execute("SELECT AVG(rate) FROM currency_conversion WHERE currency_code=? AND date > ? AND date < ?", (currency,date_start,date_end))
    result = cur.fetchone()[0]
    print (f" The exchange rate for {currency} between {date_start} and {date_end} is : {result:.4f}")

#Getting Data From APi
df = get_currency_api_data()

#Writing To SQLLite
conn = store_date_sqlite(df)

# function to calcuate the average
get_currency_average("MAD", "2021-11-19", "2021-11-26", conn)