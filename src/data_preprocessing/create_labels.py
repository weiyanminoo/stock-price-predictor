def create_labels(data, price_col="close", horizon=7, threshold=0.03):
    """
    Generate buy/sell/hold labels based on future price change.

    Parameters:
        data (pd.DataFrame): DataFrame containing stock prices with a 'close' column.
        price_col (str): Column name of stock price to use for target calculations.
        horizon (int): Number of days to look ahead for price change.
        threshold (float): Threshold percentage change for buy/sell decisions.

    Returns:
        pd.Series: Series with labels (1 for Buy, -1 for Sell, 0 for Hold).
    """
    # calculate percentage change over the specified horizon
    data["future_price"] = data[price_col].shift(-horizon)
    data["price_change"] = (data["future_price"] - data[price_col]) / data[price_col]

    # define labels based on the threshold
    data["label"] = 0  # Hold by default
    data.loc[data["price_change"] >= threshold, "label"] = 1  # Buy
    data.loc[data["price_change"] <= -threshold, "label"] = -1  # Sell

    # clean up intermediate columns
    labels = data["label"].copy()
    data.drop(columns=["future_price", "price_change", "label"], inplace=True)

    return labels
