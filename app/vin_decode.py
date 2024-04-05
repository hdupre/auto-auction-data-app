import os
import logging
import configparser
import pandas as pd
from datetime import datetime, date
from sqlalchemy import create_engine, MetaData
import json
import psycopg2
from collections import defaultdict

metadata = MetaData()

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

def get_missing_columns(df, table_name, engine):
    # Initialize metadata object
    metadata = MetaData()

    # Load the table from the database
    metadata.reflect(bind=engine, only=[table_name])
    table = metadata.tables[table_name]

    # Get existing columns from the table
    existing_columns = table.columns.keys()

    # Prepare sets for comparison
    df_columns = set(df.columns)
    existing_columns_set = set(existing_columns)

    # Adjust for potential truncation by checking if each DataFrame column
    # matches or starts any of the existing column names up to its length
    truncated_missing = set()
    for df_col in df_columns:
        if df_col not in existing_columns_set:
            # Assume potential truncation and check for startswith matches
            match_found = any(ec.startswith(df_col) for ec in existing_columns)
            if not match_found:
                truncated_missing.add(df_col)

    return truncated_missing

def handle_and_log_missing_columns(df, table_name, engine):
    missing_columns = get_missing_columns(df, table_name, engine)

    for column in missing_columns:
        # Log column name, data, and vin
        logging.warning(f"Column {column} does not exist in the table {table_name}.")
        for index, row in df.iterrows():
            logging.warning(f"VIN: {row['vin']}, {column}: {row[column]}")  # Assuming 'vin' is the column name for vins

        # Drop the column from the DataFrame
        df.drop(columns=[column], inplace=True)

    return df  # Return the modified DataFrame

def decode_vin():
    postgres_config = load_postgres_configurations()
    mssql_config = load_mssql_configurations()

    # Connect to the auto_db and fetch vins
    auto_db_connection_str = f"postgresql://{postgres_config['user']}:{postgres_config['passwd']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['db']}"
    vin_decode_db_connection_str = f"mssql+pymssql://{mssql_config['user']}:{mssql_config['passwd']}@{mssql_config['host']}:{mssql_config['port']}/{mssql_config['db']}"

    engine_auto_db = create_engine(auto_db_connection_str)
    engine_vin_decode_db = create_engine(vin_decode_db_connection_str)

    with engine_auto_db.connect() as connection_auto_db, engine_vin_decode_db.connect() as connection_vin_decode_db:
        staging_list = fetch_vins_from_staging(connection_auto_db)
        logging.info(f"Retrieved staging list. Size {len(staging_list)}")

        df_combined = pd.DataFrame()

        for i, vin in enumerate(staging_list['vin']):
            transposed = decode_single_vin(vin, connection_vin_decode_db)
            df_combined = pd.concat([df_combined, transposed], ignore_index=True).fillna("Not Applicable")

        df_combined = handle_and_log_missing_columns(df_combined, 'auction_list_decoded', engine_auto_db)
        try:
            df_combined.to_sql('auction_list_decoded', schema='public', con=engine_auto_db, if_exists='append', index=False)
            logging.info("Decoded vins loaded to db.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

def create_json():
    postgres_config = load_postgres_configurations()

    sql_query = """
    SELECT
        als.lot_number,
        als.auction_date,
        als.state,
        als.lienholder_name,
        als.borough,
        als.location_order,
        als.vin,
        NULLIF(ald."Model Year"::text, 'Not Applicable'::text) AS model_year,
        NULLIF(ald."Make"::text, 'Not Applicable'::text) AS make,
        NULLIF(ald."Model"::text, 'Not Applicable'::text) AS model,
        NULLIF(ald."Trim"::text, 'Not Applicable'::text) AS trim_level,
        NULLIF(ald."Series"::text, 'Not Applicable'::text) AS series,
        NULLIF(ald."Body Class"::text, 'Not Applicable'::text) AS body_class,
        NULLIF(ald."Drive Type"::text, 'Not Applicable'::text) AS drive_type,
        NULLIF(ald."Engine Number of Cylinders"::text, 'Not Applicable'::text) AS cylinders,
        NULLIF(ald."Displacement (L)"::text, 'Not Applicable'::text) AS displacement,
        NULLIF(ald."Fuel Type - Primary"::text, 'Not Applicable'::text) AS fuel_type,
        NULLIF(ald."Engine Configuration"::text, 'Not Applicable'::text) AS engine_configuration,
        NULLIF(ald."Base Price ($)"::text, 'Not Applicable'::text) AS base_price,
        NULLIF(ald."Transmission Style"::text, 'Not Applicable'::text) AS transmission
    FROM
        auction_list_staging als
    JOIN
        auction_list_decoded ald ON ald.vin = als.vin
    WHERE
        auction_date >= current_date
    ORDER BY
        auction_date, borough, location_order, lot_number;
    """

    try:
        conn = psycopg2.connect(
            host=postgres_config['host'],
            database=postgres_config['db'],
            user=postgres_config['user'],
            password=postgres_config['passwd'])
        cursor = conn.cursor()
        cursor.execute(sql_query)

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

    except Exception as e:
        return str(e), 500
    return

if __name__ == '__main__':
    decode_vin()
    create_json()
