import psycopg2
import configparser
from datetime import datetime
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import numpy as np

def load_postgres_configurations():
    config = configparser.ConfigParser()
    config.read('../application.properties')
    return {
        'host': config.get('postgres', 'host'),
        'port': config.get('postgres', 'port'),
        'user': config.get('postgres', 'user'),
        'password': config.get('postgres', 'passwd'),
        'database': config.get('postgres', 'db')
    }

def connect_to_database(config):
    try:
        return psycopg2.connect(**config)
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def fetch_auction_data(connection):
    cursor = connection.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    query = """
    SELECT DISTINCT d."Make", d."Model", d."Model Year" AS year
    FROM "auction_list_decoded" d
    JOIN "auction_list_staging" s ON d."vin" = s."vin"
    LEFT JOIN car_aggregates a ON (d."Make" = a.make AND d."Model" = a.model AND d."Model Year" = a.year)
    WHERE s."auction_date" >= %s AND (a.last_updated IS NULL OR a.last_updated < CURRENT_DATE - INTERVAL '6 months')
    """
    cursor.execute(query, (today,))
    return cursor.fetchall()


def setup_selenium():
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def format_url_part(text):
    """Normalize text for URL: remove special characters, spaces, convert to lower."""
    return re.sub(r'\W+', '', text).lower()

def scrape_data(driver, make, model, year):
    formatted_make = format_url_part(make)
    formatted_model = format_url_part(model)
    url = f"https://www.autotempest.com/results?make={formatted_make}&model={formatted_model}&zip=10706&localization=country&minyear={year}&maxyear={year}"
    driver.get(url)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.badge__label.label--price"))
        )
        price_elements = driver.find_elements(By.CSS_SELECTOR, "div.badge__label.label--price")
        prices = [int(price.text.strip('$').replace(',', '')) if price.text.strip('$').replace(',', '').isdigit() else 'N/A' for price in price_elements]
        mileage_elements = driver.find_elements(By.CSS_SELECTOR, ".info.mileageDate span.mileage")
        mileages = [int(mileage.text.strip(' mi.').replace(',', '')) if mileage.text.strip(' mi.').replace(',', '').isdigit() else 'N/A' for mileage in mileage_elements]
    except Exception as e:
        print(f"Failed to scrape {url}: {str(e)}")
        prices, mileages = [], []

    numeric_prices = [p for p in prices if isinstance(p, int)]
    return {
        'make': make,
        'model': model,
        'year': year,
        'prices': prices,
        'mileages': mileages,
        'max_price': max(numeric_prices) if numeric_prices else 'No Data',
        'min_price': min(numeric_prices) if numeric_prices else 'No Data',
        'median_price': np.median(numeric_prices) if numeric_prices else 'No Data',
        'max_mileage': max(mileages) if mileages else 'No Data',
        'min_mileage': min(mileages) if mileages else 'No Data',
        'median_mileage': np.median(mileages) if mileages else 'No Data'
    }

def insert_car_data(connection, data):
    cursor = connection.cursor()
    # Only proceed if there is valid data to insert
    if data['max_price'] != 'No Data' and data['min_price'] != 'No Data' and data['median_price'] != 'No Data':
        # Insert aggregate data
        cursor.execute("""
            INSERT INTO car_aggregates (make, model, year, max_price, min_price, median_price, max_mileage, min_mileage, median_mileage, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (make, model, year) DO UPDATE
            SET max_price = EXCLUDED.max_price, min_price = EXCLUDED.min_price, median_price = EXCLUDED.median_price, max_mileage = EXCLUDED.max_mileage, min_mileage = EXCLUDED.min_mileage, median_mileage = EXCLUDED.median_mileage, last_updated = CURRENT_TIMESTAMP
        """, (data['make'], data['model'], data['year'], data['max_price'], data['min_price'], data['median_price'], data['max_mileage'], data['min_mileage'], data['median_mileage']))

        # Insert individual price and mileage data
        for price, mileage in zip(data['prices'], data['mileages']):
            if isinstance(price, int) and isinstance(mileage, int):
                cursor.execute("""
                    INSERT INTO car_prices (make, model, year, price, mileage, last_updated)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (make, model, year, price, mileage) DO UPDATE
                    SET last_updated = CURRENT_TIMESTAMP
                """, (data['make'], data['model'], data['year'], price, mileage))

        connection.commit()
    else:
        print("No valid data to insert for", data['make'], data['model'], data['year'])


def main():
    config = load_postgres_configurations()
    connection = connect_to_database(config)
    if connection:
        cars = fetch_auction_data(connection)
        driver = setup_selenium()
        for make, model, year in cars:
            data = scrape_data(driver, make, model, year)
            print(data)
            insert_car_data(connection, data)
        driver.quit()
        connection.close()
    else:
        print("Failed to establish database connection.")

if __name__ == "__main__":
    main()
