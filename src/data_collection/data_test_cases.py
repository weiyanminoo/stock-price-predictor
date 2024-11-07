import pytest
from unittest import mock
from fetch_data import create_connection, fetch_stock_data, save_to_db


@pytest.fixture
def mock_connection():
    """Fixture to create a mock database connection."""
    conn = mock.MagicMock()
    return conn


def test_create_connection():
    """Test the database connection function."""
    connection = create_connection()
    assert connection is not None, "Failed to create a database connection."


def test_fetch_stock_data_success():
    """Test successful data fetching from Yahoo Finance."""
    data = fetch_stock_data("NVDA")
    assert data is not None, "Failed to fetch stock data."
    assert not data.empty, "Fetched data is empty."


def test_fetch_stock_data_failure():
    """Test handling of fetch failure with invalid ticker."""
    data = fetch_stock_data("INVALID")
    assert data is None, "Invalid ticker should return None."


def test_save_to_db(mock_connection):
    """Test saving data to the database with valid data."""
    stock_data = mock.MagicMock()
    stock_data.iterrows.return_value = [
        (
            "2024-01-01",
            {"Open": 500, "High": 520, "Low": 495, "Close": 515, "Volume": 1500000},
        )
    ]

    save_to_db(stock_data, mock_connection, "NVDA")

    # Check that the cursor was created and used correctly
    mock_connection.cursor().execute.assert_called()
    mock_connection.commit.assert_called_once()
