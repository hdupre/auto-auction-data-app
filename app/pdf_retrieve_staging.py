from bs4 import BeautifulSoup
import requests
import tabula
import logging
import pandas as pd
import re
from urllib.error import HTTPError
from sqlalchemy import create_engine
from datetime import datetime
import configparser
import os
import pdfplumber
import numpy as np
os.environ['DYLD_LIBRARY_PATH'] = '/Users/harris/Library/Java/JavaVirtualMachines/corretto-17.0.8/Contents/Home/lib'

local_pdfs = [
              "/Users/harris/Projects/auto_auction/auto-auction-data-apps/pdf/auction-101323-bronx.pdf"
              ]

config = configparser.ConfigParser()
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, '../application.properties')
config.read(config_path)

host = config.get('postgres', 'host')
port = config.get('postgres', 'port')
user = config.get('postgres', 'user')
passwd = config.get('postgres', 'passwd')
auto_db = config.get('postgres', 'db')

# Get the current date in the desired format
current_date = datetime.now().strftime('%Y-%m-%d')

# Set the log directory
log_dir = os.path.join(script_dir, '../logs')

# Create the logs directory if it doesn't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create the filename using the current date
log_filename = os.path.join(log_dir, f'pdf_retrieve_staging_{current_date}.log')

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s]: %(message)s')

logging.info("Script started")

# Define constants
BOROUGHS = ['bronx', 'brooklyn', 'statenisland', 'queens', 'manhattan']
COLUMN_NAMES = ['#', 'YEAR', 'MAKE', 'PLATE#', 'ST', 'VEHICLE ID', 'LIENHOLDER']
URL = "https://www.nyc.gov/site/finance/vehicles/auctions.page"
START_STRING = "https://www.nyc.gov"
# Define regex patterns
VIN_PATTERN = r"^(?:(?=.*[A-HJ-NPR-Z])(?=.*\d)[A-HJ-NPR-Z\d]{11,17}|[A-HJ-NPR-Z\d]{8}[\dX][A-HJ-NPR-Z\d]{2}\d{6})$"
DIGIT_PATTERN = r"(\b\d{1,2}\b)"
YEAR_PATTERN = r"\b(19[6-9]\d|20[0-1]\d|202[0-9])\b"
PLATE_PATTERN = r"(\b[a-zA-Z0-9]{6,8}\b)"
ST_PATTERN = r"(\b[A-Z]{2}\b)"
DB_CONNECTION_STRING = f'postgresql://{user}:{passwd}@{host}:{port}/{auto_db}'

def fetch_html_content(url):
    return requests.get(url).text

def extract_urls_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    abstract = soup.find_all(class_='abstract')
    return [a['href'] for a in abstract[0].find_all('a', href=True) if a['href'].endswith('.pdf')]
def append_start_string_to_urls(urls, start_string):
    return [start_string + url for url in urls]

def fetch_loaded_urls_from_db(connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            cursor.execute('SELECT url FROM url_list')
            results = cursor.fetchall()
            return [i[0] for i in results]


def get_filtered_urls(all_urls, loaded_urls):
    return [url for url in all_urls if url not in loaded_urls]


def get_auction_url_list():
    html_content = fetch_html_content(URL)
    extracted_urls = extract_urls_from_html(html_content)
    preprocessed_urls = append_start_string_to_urls(extracted_urls, START_STRING)
    loaded_urls = fetch_loaded_urls_from_db(DB_CONNECTION_STRING)
    filtered_urls = get_filtered_urls(set(preprocessed_urls), set(loaded_urls))

    logging.info("Returning URL List...")
    return filtered_urls


def download_pdf(url, directory="../pdf"):
    try:
        # Create directory if not exists
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Download the file from `url`
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code

        # Check if the content type is PDF
        if 'application/pdf' not in response.headers.get('content-type', ''):
            logging.error(f"URL does not point to a PDF file: {url}")
            return None

        # Extract filename from Content-Disposition header or URL
        content_disposition = response.headers.get('content-disposition')
        if content_disposition:
            filename = re.findall('filename=(.+)', content_disposition)[0]
        else:
            filename = url.split("/")[-1]

        # Create full path
        full_path = os.path.join(directory, filename)

        # Save the PDF
        with open(full_path, 'wb') as out_file:
            out_file.write(response.content)

        logging.info(f"File saved to {full_path}")
        return full_path

    except requests.RequestException as e:
        logging.error(f"Error downloading the file: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def extract_text_from_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = ''
        for page in pdf.pages:
            text += page.extract_text()
    return text

def manual_extraction(pdf):
    pdf_text = extract_text_from_pdf(pdf)
    rows = pdf_text.split('\n')
    processed_rows = []
    for row in rows:
        vin_match = re.search(VIN_PATTERN, row)

        # Only process rows with a VIN
        if vin_match:
            vin = vin_match.group()
            potential_lienholder = row.split(vin)[1].strip() if vin in row else ""

            # Check if potential_lienholder contains any alphanumeric characters
            if re.search(r"\w", potential_lienholder):
                lienholder = potential_lienholder
            else:
                lienholder = np.nan

            digit_match = re.search(DIGIT_PATTERN, row)
            year_match = re.search(YEAR_PATTERN, row)
            plate_match = re.search(PLATE_PATTERN, row)
            st_match = re.search(ST_PATTERN, row)

            processed_rows.append({
                '#': digit_match.group() if digit_match else np.nan,
                'YEAR': year_match.group() if year_match else np.nan,
                'MAKE': np.nan,  # Not extracting MAKE as it's complex
                'PLATE#': plate_match.group() if plate_match else np.nan,
                'ST': st_match.group() if st_match else np.nan,
                'VEHICLE ID': vin,
                'LIENHOLDER': lienholder
            })

    return pd.DataFrame(processed_rows, columns=COLUMN_NAMES)
def process_pdf(pdf):
    full_path = download_pdf(pdf)
    try:
        car_list = tabula.read_pdf(pdf, pages='all', output_format="dataframe")
        df = car_list[0]
    except:
        logging.warning("Tabula failed to read the PDF. Using manual extraction...")
        df = manual_extraction(full_path)
    else:
        if df.columns.values.tolist() != COLUMN_NAMES:
            logging.error(pdf + " unexpected column headers, extracting manually...")
        df = manual_extraction(full_path)
    return df


def process_auction_date(pdf):
    date_match = re.findall(r'(\d{6,8})', pdf)[0]
    date_format = "%m%d%y" if len(date_match) == 6 else "%m%d%Y"
    return pd.to_datetime(date_match, format=date_format)


def process_borough(pdf):
    for borough in BOROUGHS:
        if borough in pdf:
            return borough
    return None


def process_location_order(pdf):
    pattern = r"(?<=([-_]))\d(?=([-_]))"
    match_order = re.search(pattern, pdf)
    return int(match_order.group()) if match_order else 1

def adjust_lot_numbers(df):
    """
    Adjusts the lot numbers in the DataFrame if they reset after 100, considering that lot numbers start at 1.
    If the adjustment condition is not met, log the row, remove it from the DataFrame, and move on.
    """
    rows_to_drop = []  # Initialize a list to keep track of row indices to drop
    for i, row in df.iterrows():
        lot_number = int(row['#']) if pd.notnull(row['#']) else -1
        # Adjust the comparison to account for the off-by-one difference between lot numbers and row indices
        if lot_number != (i + 1):
            if not (lot_number == (i + 1) % 100 or (lot_number == 0 and (i + 1) % 100 == 100)):
                # If the condition is not met, log the error and mark the row for removal
                logging.error(f"Lot number mismatch or invalid condition for row: {row.to_dict()}")
                rows_to_drop.append(i)  # Add the index of the row to be dropped
                continue  # Skip further processing for this row
            else:
                # If the condition is met, adjust the lot number correctly
                df.at[i, 'lot_number'] = i + 1
    # Drop the rows that did not meet the condition
    df.drop(rows_to_drop, inplace=True)
    return df

def create_auction_df(url_list):
    if not url_list:
        return []

    df_combined = pd.DataFrame(columns=COLUMN_NAMES + ['auction_date', 'borough', 'location_order', 'url'])
    load_urls = []
    now = datetime.now()

    for pdf in url_list:
        try:
            df = process_pdf(pdf)
            df['auction_date'] = process_auction_date(pdf)
            df['borough'] = process_borough(pdf)
            df['location_order'] = process_location_order(pdf)
            df['url'] = pdf

            df = df[df['VEHICLE ID'].notnull() & (df['VEHICLE ID'] != 'VEHICLE ID')]
            df = adjust_lot_numbers(df)  # Adjust lot numbers here
            # Drop columns in df that are entirely empty or filled with NAs, if any, before concatenation
            # This step assumes you want to retain the column structure of df_combined
            columns_to_keep = df_combined.columns.intersection(df.columns)
            df_filtered = df[columns_to_keep].dropna(how='all', axis=1)

            # Concatenate while retaining the structure of df_combined
            df_combined = pd.concat([df_combined, df_filtered], ignore_index=True).reindex(columns=df_combined.columns)
            load_urls.append([pdf, "loaded_url", now])

        except (HTTPError, IndexError, ValueError, Exception) as err:
            load_urls.append([pdf, type(err).__name__, now])
            logging.error(f"{type(err).__name__} on {pdf}: {err}")

    df_combined.rename(columns={
        "#": "lot_number",
        "YEAR": "model_year",
        "MAKE": "make",
        "PLATE#": "license_plate",
        "ST": "state",
        "VEHICLE ID": "vin",
        "LIENHOLDER": "lienholder_name"
    }, inplace=True)

    url_list_df = pd.DataFrame(load_urls, columns=['url', 'status', 'process_time'])
    return [df_combined, url_list_df]

def load_auction_db(df_list):

    if len(df_list) == 0:
        logging.info("No new auctions")
        return

    engine = create_engine('postgresql://' + user + ':' + passwd + '@' + host + ':'+port+'/' + auto_db)

    try:
        df_list[0].to_sql('auction_list_staging', schema='public', con=engine, if_exists='append',index=False)
        logging.info("Auction list loaded to database.")
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)

    try:
        df_list[1].to_sql('url_list',schema='public', con=engine, if_exists='append',index=False)
        logging.info("URL list loaded to database.")
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)

    engine.dispose()


if __name__ == '__main__':
    if False:
        df_list = create_auction_df(local_pdfs)
        load_auction_db(df_list)
        # Print the main dataframe (assuming it's the first in the list)
        logging.info("Manual run complete.")
    else:
        url_list = get_auction_url_list()
        df_list = create_auction_df(url_list)
        load_auction_db(df_list)
        logging.info("Script completed.")
