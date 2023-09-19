import os
import logging
import configparser
import pandas as pd
from datetime import datetime, date
from sqlalchemy import create_engine
import json
import psycopg2
from collections import defaultdict

# Get the current date in the desired format
current_date = datetime.now().strftime('%Y-%m-%d')

# Set the log directory (assuming you've already defined it)
log_dir = '../logs'  # example log directory

# Create the filename using the current date
log_filename = os.path.join(log_dir, f'decode_vin_{current_date}.log')

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s]: %(message)s')

logging.info("Script started")

def date_handler(obj):
    """
    Handles JSON serialization for date and datetime objects.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def load_postgres_configurations():
    config = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '../application.properties')
    config.read(config_path)

    db_config = {
        'host': config.get('postgres', 'host'),
        'port': config.get('postgres', 'port'),
        'user': config.get('postgres', 'user'),
        'passwd': config.get('postgres', 'passwd'),
        'db': config.get('postgres', 'db')
    }

    return db_config

def load_mssql_configurations():
    config = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '../application.properties')
    config.read(config_path)

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
    auto_db_connection_str = f"postgresql://{postgres_config['user']}:{postgres_config['passwd']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['db']}"
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
    postgres_config = load_postgres_configurations()

    try:
        conn = psycopg2.connect(
            host=postgres_config['host'],
            database=postgres_config['db'],
            user=postgres_config['user'],
            password=postgres_config['passwd'])
        cursor = conn.cursor()
        cursor.execute('SELECT vin, "Model Year" AS model_year, "Make" AS make, "Model" AS model, '
                       'CASE WHEN "Trim" = \'Not Applicable\' AND "Series" = \'Not Applicable\' THEN NULL '
                       'ELSE TRIM(CONCAT(CASE WHEN "Series" = \'Not Applicable\' THEN \'\' ELSE "Series" END, \' \', '
                       'CASE WHEN "Trim" = \'Not Applicable\' THEN \'\' ELSE "Trim" END)) '
                       'END AS series_trim, auction_date, lot_number, '
                       'state, lienholder_name, borough, location_order '
                       'FROM v_auction_list '
                       'WHERE auction_date >= CURRENT_DATE '
                       'ORDER BY auction_date, borough, location_order , lot_number;')

        columns = [x[0] for x in cursor.description]
        rows = cursor.fetchall()

        grouped_data = defaultdict(list)
        for result in rows:
            record = dict(zip(columns, result))
            global_key = (record['auction_date'], record['borough'], record['location_order'])
            grouped_data[global_key].append(record)

        optimized_data = []
        for global_key, records in grouped_data.items():
            global_attributes = {
                "auction_date": global_key[0],
                "borough": global_key[1],
                "location_order": global_key[2]
            }

            transformed_records = {}
            for key in records[0].keys():
                if key not in global_attributes:
                    transformed_records[key] = [item[key] for item in records]

            group_data = {
                "global": global_attributes,
                "records": transformed_records
            }
            optimized_data.append(group_data)

        cursor.close()
        conn.close()
        # Ensure /data directory exists
        data_directory = '../data'
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)

        # Write to JSON file in /data directory at the project root
        try:
            with open(os.path.join(data_directory, 'output.json'), 'w') as outfile:
                json.dump(optimized_data, outfile, indent=4, default=date_handler)
        except Exception as file_write_error:
            return f"Error writing to file: {file_write_error}", 500

        # Print a portion of the data to console for verification
        print(optimized_data[:5])

    except Exception as e:
        return str(e), 500
    return

if __name__ == '__main__':
    decode_vin()
    create_json()
