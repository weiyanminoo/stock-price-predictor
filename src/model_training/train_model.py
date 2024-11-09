import logging
import joblib
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from src.data_preprocessing.create_labels import create_labels
from src.data_preprocessing.preprocess_data import preprocess_data, load_data_from_db

# configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# database connection string
connection_string = (
    "postgresql+psycopg2://postgres:n0mAd248!0wy@localhost:5432/stock_data"
)


def load_and_prepare_data(ticker):
    """Load and preprocess stock data from the database."""
    logging.info("Loading and preparing data...")
    engine = create_engine(connection_string)
    raw_data = load_data_from_db(engine, ticker)
    logging.info("Raw data loaded successfully.")

    preprocessed_data = preprocess_data(raw_data)
    logging.info("Data preprocessed successfully.")

    labels = create_labels(preprocessed_data)
    logging.info("Labels created successfully.")

    preprocessed_data["label"] = labels
    logging.info("Data prepared with labels.")
    return preprocessed_data


def train_and_evaluate_model(data):
    """Train a classification model and evaluate it on a test set."""
    logging.info("Starting model training and evaluation...")

    # separate features and labels
    X = data.drop(columns=["label"])
    y = data["label"]

    # split into training and testing sets
    logging.info("Splitting data into train and test sets...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # initialize the model
    model = RandomForestClassifier(random_state=42)

    # train the model
    logging.info("Training the RandomForest model...")
    model.fit(X_train, y_train)

    # make predictions on the test set
    logging.info("Making predictions on the test set...")
    y_pred = model.predict(X_test)

    # evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    logging.info(f"Model Accuracy: {accuracy}")
    logging.info("Classification Report:\n" + report)

    return model


def save_model(model, model_filename="stock_predictor_model.pkl"):
    """Save the trained model to a file."""
    logging.info(f"Saving the model to {model_filename}...")
    joblib.dump(model, model_filename)
    logging.info(f"Model saved successfully to {model_filename}")


def main():
    logging.info("Starting main training process...")
    ticker = "NVDA"  # use the stock ticker you want to train on

    # load and prepare data
    logging.info(f"Loading and preparing data for ticker: {ticker}")
    data = load_and_prepare_data(ticker)

    # train the model
    logging.info("Training and evaluating the model...")
    model = train_and_evaluate_model(data)

    # save the model
    logging.info("Saving the trained model...")
    save_model(model)
    logging.info("Training process complete.")


if __name__ == "__main__":
    main()

# run using:
# python -m src.model_training.train_model
