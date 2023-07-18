from data_pyetl.connectors import DB_Connector
from data_pyetl.database_helper import DbHelper
from data_pyetl.api_helper import APIHelper
from data_pyetl.file_helper import FileHelper

import pandas as pd
import requests
import wget
import os


def get_dw_engine():

    ds_con = "Postgres"
    credentials_dw = {
        "host": "0.0.0.0",
        "db": "postgres",
        "username": "postgres",
        "pwd": "d4t4b3s&p0stgR3$",
        "port": "5432"
    }

    dbcon_dw = DB_Connector(ds_con, credentials_dw)
    engine_dw = dbcon_dw.create_data_source_connection()

    return engine_dw


def get_data_from_db():
    ds_con = "Postgres"
    credentials_origin = {
        "host": "34.173.103.16",
        "db": "postgres",
        "username": "junior",
        "pwd": "|?7LXmg+FWL&,2(",
        "port": "5432"
    }

    dbcon_origin = DB_Connector(ds_con, credentials_origin)
    engine_origin = dbcon_origin.create_data_source_connection()
    engine_dw = get_dw_engine()

    dbhelper = DbHelper(table_name="venda", columns="*",
                        ds_engine=engine_origin, query="SELECT * FROM public.venda")

    inserted_dw = dbhelper.insert_dw(engine_dw, "Stage_", "public")

    return inserted_dw


def get_data_from_api():
    origin_url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior"
    api_helper = APIHelper(origin_url, request_headers={}, request_data={})
    engine_dw = get_dw_engine()

    df_list = list()
    for i in range(1, 10):
        response = requests.get(origin_url + f"{i}")
        df_list.append(response.text)

    df = pd.DataFrame({
        'id': list(range(1, 10)),
        'funcionario': df_list
    })
    inserted_dw = api_helper.dataframe_to_dw(
        df, "Stage_", "Funcionario", engine_dw, "public")

    return inserted_dw


def get_data_from_parquet():
    origin_url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
    engine_dw = get_dw_engine()

    if os.path.exists("categoria.parquet"):
        os.remove("categoria.parquet")

    wget.download(origin_url, "categoria.parquet")

    file_helper = FileHelper("./", "parquet")
    df = file_helper.read_parquet_to_dataframe()

    inserted_dw = file_helper.dataframe_to_db(
        df, "Stage_", "Categoria", engine_dw, "public")

    return inserted_dw
