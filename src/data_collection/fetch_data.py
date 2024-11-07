import yfinance as yf
import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.errors import UniqueViolation
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname="stock_data",
            user="postgres",
            password="n0mAd248!0wy",
            host="localhost",
            port="5432",
        )
        logging.info("Connected to the database successfully.")
        return connection
    except OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None


def fetch_stock_data(ticker):
    """Fetch stock data from Yahoo Finance."""
    try:
        stock_data = yf.download(ticker)
        logging.info(f"Data fetched successfully for {ticker}.")
        return stock_data
    except Exception as e:
        logging.error(f"Failed to fetch data for {ticker}: {e}")
        return None


def save_to_db(data, connection, ticker):
    """Save stock data to PostgreSQL database."""
    cursor = connection.cursor()
    create_table_query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS stock_data (
            ticker VARCHAR(10),
            date DATE,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            PRIMARY KEY (ticker, date)
        )
    """
    )
    cursor.execute(create_table_query)

    insert_query = sql.SQL(
        """
        INSERT INTO stock_data (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date) DO NOTHING
    """
    )

    # Replace NaN with None across all columns for easy DB insertion
    data = data.astype(object).where(pd.notna(data), None)

    for index, row in data.iterrows():
        date = index.to_pydatetime()

        # Ensure that each value is strictly a scalar by using `.item()` if needed
        open_price = row["Open"] if row["Open"] is None else row["Open"].item()
        high_price = row["High"] if row["High"] is None else row["High"].item()
        low_price = row["Low"] if row["Low"] is None else row["Low"].item()
        close_price = row["Close"] if row["Close"] is None else row["Close"].item()
        volume = row["Volume"] if row["Volume"] is None else int(row["Volume"])

        try:
            cursor.execute(
                insert_query,
                (ticker, date, open_price, high_price, low_price, close_price, volume),
            )
            logging.info(f"Inserted data for {ticker} on {date}.")
        except UniqueViolation:
            logging.warning(f"Duplicate entry for {ticker} on {date}. Skipping.")
        except Exception as e:
            logging.error(f"Failed to insert data for {ticker} on {date}: {e}")

    connection.commit()
    cursor.close()


def main():
    ticker = "NVDA"  # NVIDIA stock ticker symbol
    connection = create_connection()

    if connection is not None:
        stock_data = fetch_stock_data(ticker)
        if stock_data is not None:
            save_to_db(stock_data, connection, ticker)
        connection.close()


if __name__ == "__main__":
    main()
