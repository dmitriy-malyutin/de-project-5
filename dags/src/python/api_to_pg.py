import requests
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine


def api_to_pg(api_key, api_url, POSTGRS_HOST, POSTGRS_DB_NAME, POSTGRS_USER, POSTGRS_PASSWORD):
    methods = ['restaurants', 'couriers', 'deliveries']

    headers = {
        'X-Nickname': 'Dmitriy',
        'X-Cohort': '10',
        'X-API-KEY': api_key
    }

    def get_restaurants():
        url = f'{api_url}/{methods[0]}?sort_field=0&sort_direction=asc&limit=0&offset=0'
        resp = requests.get(url, headers=headers)
        restaurants = resp.json()
        return restaurants

    def get_couriers():
        url = f'{api_url}/{methods[1]}?sort_field=0&sort_direction=asc&limit=0&offset=0'
        resp = requests.get(url, headers=headers)
        couriers = resp.json()
        return couriers

    def get_deliveries():
        url = f'{api_url}/{methods[2]}?sort_field=0&sort_direction=asc&limit=0&offset=0'
        resp = requests.get(url, headers=headers)
        deliveries = resp.json()
        return deliveries

    df_restaurants = pd.json_normalize(get_restaurants())
    df_couriers = pd.json_normalize(get_couriers())
    df_deliveries = pd.json_normalize(get_deliveries())

    dbConnection = create_engine(f"postgresql+psycopg2://{POSTGRS_USER}:{POSTGRS_PASSWORD}@{POSTGRS_HOST}:5432/{POSTGRS_DB_NAME}")

    df_restaurants.to_sql('restaurants', dbConnection, schema='stg', if_exists='append', index=False)
    df_couriers.to_sql('couriers', dbConnection, schema='stg', if_exists='append', index=False)
    df_deliveries.to_sql('deliveries', dbConnection, schema='stg', if_exists='append', index=False)