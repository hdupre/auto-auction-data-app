import os
import logging
import configparser
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import json
import psycopg2
from collections import defaultdict

# Get the current date in the desired format
current_date = datetime.now().strftime('%Y-%m-%d')

# Set the log directory (assuming you've already defined it)
log_dir = './logs'  # example log directory

# Create the filename using the current date
log_filename = os.path.join(log_dir, f'decode_vin_{current_date}.log')

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s]: %(message)s')

logging.info("Script started")

def load_postgres_configurations():
    config = configparser.ConfigParser()
    config.read('application.properties')

    db_config = {
        'host': config.get('postgres', 'host'),
        'user': config.get('postgres', 'user'),
        'passwd': config.get('postgres', 'passwd'),
        'auto_db': config.get('postgres', 'auto_db')
    }

    return db_config

def load_mssql_configurations():
    config = configparser.ConfigParser()
    config.read('application.properties')

    db_config = {
        'host': config.get('mssql', 'host'),
        'port': config.get('mssql', 'port'),
        'user': config.get('mssql', 'user'),
        'passwd': config.get('mssql', 'passwd'),
        'db': config.get('mssql', 'db')
    }

    return db_config


def fetch_vins_from_staging(engine):
    sql = """
    SELECT DISTINCT a.vin 
    FROM auction_list_staging a 
    LEFT JOIN auction_list_decoded b on a.vin = b.vin 
    WHERE b.vin IS NULL
    """
    return pd.read_sql(sql, con=engine)


def decode_single_vin(vin, engine_decode):
    nhtsa_stored_proc = "EXEC [dbo].[spVinDecode] @v = %s"
    decoded = pd.read_sql_query(nhtsa_stored_proc, engine_decode, params=(vin,))
    col_list = ['Variable', 'Value']
    transposed = decoded[col_list].transpose()
    transposed.columns = transposed.iloc[0]
    transposed = transposed[1:]
    transposed.insert(loc=0, column='vin', value=vin)
    transposed = transposed.loc[:, ~transposed.columns.duplicated()].copy()
    return transposed


def decode_vin():
    postgres_config = load_postgres_configurations()
    mssql_config = load_mssql_configurations()

    # Connect to the auto_db and fetch vins
    auto_db_connection_str = f"postgresql://{postgres_config['user']}:{postgres_config['passwd']}@{postgres_config['host']}/{postgres_config['auto_db']}"
    vin_decode_db_connection_str = f"mssql+pymssql://{mssql_config['user']}:{mssql_config['passwd']}@{mssql_config['host']}:{mssql_config['port']}/{mssql_config['db']}"

    with create_engine(auto_db_connection_str).connect() as engine, create_engine(
            vin_decode_db_connection_str).connect() as engine_decode:
        staging_list = fetch_vins_from_staging(engine)
        logging.info(f"Retrieved staging list. Size {len(staging_list)}")

        df_combined = pd.DataFrame()

        for i, vin in enumerate(staging_list['vin']):
            transposed = decode_single_vin(vin, engine_decode)
            df_combined = pd.concat([df_combined, transposed], ignore_index=True).fillna("Not Applicable")

        try:
            df_combined.to_sql('auction_list_decoded', schema='public', con=engine, if_exists='append', index=False)
            logging.info("Decoded vins loaded to db.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

def create_json():
    return

if __name__ == '__main__':
    decode_vin()
    # create_json()
