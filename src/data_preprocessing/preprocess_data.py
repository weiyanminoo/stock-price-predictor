import logging
from sqlalchemy import create_engine
import pandas as pd
from sklearn.preprocessing import StandardScaler
from src.data_preprocessing.create_labels import create_labels

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
    # convert date to datetime and set as index
    data["date"] = pd.to_datetime(data["date"])
    data.set_index("date", inplace=True)

    # handle missing values - forward fill or interpolate
    data.ffill(inplace=True)

    # create additional date features
    data["day_of_week"] = data.index.dayofweek  # Monday=0, Sunday=6
    data["is_month_start"] = data.index.is_month_start.astype(int)
    data["is_month_end"] = data.index.is_month_end.astype(int)

    # calculate technical indicators
    data["SMA_7"] = (
        data["close"].rolling(window=7).mean()
    )  # 7-day Simple Moving Average
    data["SMA_21"] = (
        data["close"].rolling(window=21).mean()
    )  # 21-day Simple Moving Average
    data["volatility"] = (
        data["close"].rolling(window=7).std()
    )  # 7-day rolling standard deviation (volatility)
    data["momentum"] = data["close"].diff(7)  # 7-day momentum

    # drop ticker column
    data.drop(columns=["ticker"], inplace=True)

    # standardize numerical columns
    scaler = StandardScaler()
    numerical_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "SMA_7",
        "SMA_21",
        "volatility",
        "momentum",
    ]
    data[numerical_columns] = scaler.fit_transform(data[numerical_columns])

    # drop rows with NaN values created by rolling windows
    data.dropna(inplace=True)

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

        # call create_labels to generate labels
        labels = create_labels(preprocessed_data)
        preprocessed_data["label"] = labels

        print(preprocessed_data.head())
        engine.dispose()


if __name__ == "__main__":
    main()

# run using this:
# python -m src.data_preprocessing.preprocess_data
