import logging
from sqlalchemy import create_engine
import pandas as pd
from sklearn.preprocessing import StandardScaler

# connection string
connection_string = (
    "postgresql+psycopg2://postgres:n0mAd248!0wy@localhost:5432/stock_data"
)


def load_data_from_db(engine, ticker):
    """load stock data from PostgreSQL database using the SQLAlchemy engine."""
    query = f"SELECT * FROM stock_data WHERE ticker = '{ticker}'"
    # use the engine to read data
    data = pd.read_sql(query, engine)
    return data


def preprocess_data(data):
    """Preprocess the stock data for model training."""
    # convert date to datetime
    data["date"] = pd.to_datetime(data["date"])

    # set the date as the index for time-series processing
    data.set_index("date", inplace=True)

    # handle missing values - forward fill or interpolate
    data.ffill(inplace=True)

    # create additional date features
    data["day_of_week"] = data.index.dayofweek  # Monday=0, Sunday=6
    data["is_month_start"] = data.index.is_month_start.astype(int)
    data["is_month_end"] = data.index.is_month_end.astype(int)

    # drop ticker column
    data.drop(columns=["ticker"], inplace=True)

    # standardize numerical columns
    scaler = StandardScaler()
    numerical_columns = ["open", "high", "low", "close", "volume"]
    data[numerical_columns] = scaler.fit_transform(data[numerical_columns])

    return data


def create_connection():
    """establish a connection to the PostgreSQL database using SQLAlchemy."""
    try:
        # create the engine (we can reuse this engine for multiple queries)
        engine = create_engine(connection_string)
        logging.info("Connected to the database successfully using SQLAlchemy.")
        return engine
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None


def main():
    # connect to the database using SQLAlchemy engine
    engine = create_connection()

    # load and preprocess data
    if engine is not None:
        ticker = "NVDA"
        raw_data = load_data_from_db(engine, ticker)
        preprocessed_data = preprocess_data(raw_data)

        print(preprocessed_data.head())
        # Do not need to manually close the engine, it's managed by SQLAlchemy


if __name__ == "__main__":
    main()

# run using this:
# python -m src.data_preprocessing.preprocess_data
