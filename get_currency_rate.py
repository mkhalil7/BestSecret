# The goal of this class is to provide a pipeline to retireve historical exchange rate data
# The base currency as well as the start and end date can be configured as parameters


# the configatuin parametrs are present in the config.ini file

import pandas as pd
import requests
import configparser


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


# send an API request and add the data
def query_api():

    if(len(api_key) == 0 or len(api_url) == 0):
        raise Exception('Missing api URL or token please check the configuration file')

    params = {
        'access_key': api_key,
        'base': base_currency,
        'start_date': start_date,
        'end_date': end_date
    }

    #sending the request to the API 
    response = requests.get(api_url,params = params)
    response_json = response.json()
    
   # print(response_json)
    if(response.status_code == 200 and response_json['success'] or True):

        #loading everything to data frame 
        df = pd.DataFrame.from_dict(pd.json_normalize(response_json),  orient='columns')

        #removing unecessary columns 
        del df['success']
        del df['timeseries']
        del df['start_date']
        del df['end_date']


        #using melt to unpivot table
        df = pd.melt(df, id_vars=["base"], var_name="temp", value_name="Value")
        df[['Rate','Date', 'Currency']] = df['temp'].str.split('.', 2, expand=True)

        #cleaning and reorganizing 
        df.columns = df.columns.str.replace('base', 'Base')
        df = df[['Date','Base', 'Currency', 'Value']]
   
        print(df)
    else:   
        raise Exception('API query failed with folowing error message :' + response_json['error']['info'])


    
   # print(api_token, api_url)

    # to be fixed


# store the data into a sqlite database
def store_date_sqlite():
    pass


query_api()