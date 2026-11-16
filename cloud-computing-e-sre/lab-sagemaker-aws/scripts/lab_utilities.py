# Import required preprocessing libraries
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from datetime import timedelta
import boto3
import numpy as np
import sagemaker
import matplotlib.pyplot as plt
import pandas as pd
import xgboost as xgb
# from sagemaker.remote_function import remote
import pandas as pd


def process_data(data: pd.DataFrame, prediction_horizon="1y"):
    """
    Process and prepare sales data for machine learning model
    
    Args:
        data (DataFrame): Raw sales data
        prediction_horizon (str): Time period to predict ("1y", "3m", "15d")
        
    Returns:
        Tuple of processed datasets and metadata
    """

    data = data.fillna(0)
    # Create copy to avoid modifying original
    df = data.copy()

    # Convert date columns to datetime
    df["order_date"] = pd.to_datetime(df["order_date"])

    # Sort by date
    df = df.sort_values("order_date")

    # Create time-based features
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month
    df["quarter"] = df["order_date"].dt.quarter
    df["month_of_quarter"] = df["month"] % 3 + 1
    df["day_of_month"] = df["order_date"].dt.day
    df["day_of_week"] = df["order_date"].dt.dayofweek
    df["week_of_year"] = df["order_date"].dt.isocalendar().week.astype(int)
    # Add a date column for easier date calculations
    df["date"] = df["order_date"].dt.date

    # Create sales history features
    for i in range(1, 4):
        # Lag features for units sold
        df[f"units_sold_lag_{i}"] = df.groupby(["item_type", "sales_channel"])[
            "units_sold"
        ].shift(i)

        # Rolling mean features
        df[f"units_sold_rolling_mean_{i}m"] = df.groupby(
            ["item_type", "sales_channel"]
        )["units_sold"].transform(
            lambda x: x.rolling(window=i * 30, min_periods=1).mean()
        )

    # Drop unnecessary columns
    columns_to_drop = ["order_id", "ship_date", "order_date", "active_promotions"]
    df = df.drop(columns_to_drop, axis=1)

    # Drop rows with NaN values
    df = df.dropna()

    # Convert categorical variables to dummy variables
    categorical_columns = [
        "region",
        "country",
        "item_type",
        "product_category",
        "sales_channel",
        "order_priority",
    ]

    # One-hot encode all categorical columns
    df_encoded = pd.get_dummies(df, columns=categorical_columns)

    # Determine prediction horizon
    horizon_multiplier = int(prediction_horizon[:-1])
    #print("horizon_multiplier")
    #print(horizon_multiplier)
    horizon_unit = prediction_horizon[-1]
    #print("horizon_unit")
    #print(horizon_unit)

    # Get the max date in the dataset
    max_date = df["date"].max()

    # Calculate future dates based on the prediction horizon
    if horizon_unit == "y":
        total_days = 365 * horizon_multiplier
        #print(f"Total days: {total_days}")
        future_dates = [
            max_date + timedelta(days=i) for i in range(1, total_days + 1)
        ]
    elif horizon_unit == "m":
        total_days = 30 * horizon_multiplier
        #print(f"Total days: {total_days}")
        future_dates = [
            max_date + timedelta(days=i) for i in range(1, total_days + 1)
        ]
    elif horizon_unit == "d":
        total_days = horizon_multiplier
        #print(f"Total days: {total_days}")
        future_dates = [
            max_date + timedelta(days=i) for i in range(1, total_days + 1)
        ]
    else:
        raise ValueError(
            f"Unsupported horizon unit: {horizon_unit}. Use 'y', 'm', or 'd'."
        )

    # Create a cutoff date for training/validation/test split
    cutoff_date = future_dates[0] - timedelta(
        days=30
    )  # 30 days before first prediction date
    validation_date = cutoff_date - timedelta(
        days=30
    )  # 30 days before cutoff for validation

    # Create masks for train/validation/test split
    train_mask = df_encoded["date"] < validation_date
    val_mask = (df_encoded["date"] >= validation_date) & (
        df_encoded["date"] < cutoff_date
    )
    test_mask = df_encoded["date"] >= cutoff_date

    # Prepare features and target
    target_column = "units_sold"
    numeric_columns = [
        "unit_price",
        "unit_cost",
        "total_revenue",
        "total_cost",
        "total_profit",
    ]

    feature_columns = [
        col for col in df_encoded.columns if col != target_column and col != "date"
    ]
    X = df_encoded[feature_columns]
    y = df_encoded[target_column].astype(float)

    X_train = X[train_mask].copy()
    y_train = y[train_mask]

    # Handle cases with no validation or test data
    if val_mask.sum() > 0:
        X_val = X[val_mask].copy()
        y_val = y[val_mask]
    else:
        # If no validation data, use a subset of training data
        from sklearn.model_selection import train_test_split

        X_train, X_val, y_train, y_val = train_test_split(
            X_train, y_train, test_size=0.2, random_state=42
        )

    if test_mask.sum() > 0:
        X_test = X[test_mask].copy()
        y_test = y[test_mask]
    else:
        # If no test data, create a dummy test set
        X_test = X_train.iloc[:0].copy()
        y_test = y_train.iloc[:0]

    # Scale numeric features
    scaler = StandardScaler()

    # Convert all integer and numeric columns to float before scaling
    numeric_features = [
        col
        for col in X_train.columns
        if col
        in numeric_columns
        + [
            "year",
            "month",
            "quarter",
            "month_of_quarter",
            "day_of_month",
            "day_of_week",
            "week_of_year",
        ]
        + [f"units_sold_lag_{i}" for i in range(1, 4)]
        + [f"units_sold_rolling_mean_{i}m" for i in range(1, 4)]
    ]

    # Convert all numeric columns to float in all dataframes
    for col in numeric_features:
        X_train[col] = X_train[col].astype(float)
        if len(X_val) > 0:
            X_val[col] = X_val[col].astype(float)
        if len(X_test) > 0:
            X_test[col] = X_test[col].astype(float)

    # Only scale if there are numeric features and data
    if numeric_features and len(X_train) > 0:
        X_train.loc[:, numeric_features] = scaler.fit_transform(
            X_train[numeric_features]
        )

    if numeric_features and len(X_val) > 0:
        X_val.loc[:, numeric_features] = scaler.transform(X_val[numeric_features])

    if numeric_features and len(X_test) > 0:
        X_test.loc[:, numeric_features] = scaler.transform(X_test[numeric_features])

    return (
        X_train,
        X_val,
        X_test,
        y_train,
        y_val,
        y_test,
        feature_columns,
        scaler,
        df_encoded,
        future_dates,
    )


def model_train_eval(X_train, y_train, X_val, y_val, feature_columns):
    """
    Train and evaluate XGBoost model for sales prediction
    
    Args:
        X_train (DataFrame): Training feature data
        y_train (Series): Training target values
        X_val (DataFrame): Validation feature data
        y_val (Series): Validation target values
        feature_columns (list): Names of features used in the model
    
    Returns:
        XGBRegressor: Trained XGBoost model
    """

    # Initialize XGBoost regressor with optimized hyperparameters
    model = xgb.XGBRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=7,
        min_child_weight=3,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric=["rmse", "mae"],
    )

    # Train the model with validation monitoring
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_train, y_train), (X_val, y_val)],
        verbose=True,
    )

    # Calculate and sort feature importance (not used but useful for analysis)
    feature_importance = pd.DataFrame(
        {"feature": feature_columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)

    return model


def predict_sales(model, df_encoded, future_dates, target_region=None):
    """
    Generate sales predictions for future dates with realistic variations
    
    Args:
        model: Trained XGBoost model
        df_encoded: Preprocessed and encoded historical sales data
        future_dates: List of dates to predict
        target_region: Optional specific region to analyze
    
    Returns:
        DataFrame with detailed sales predictions
    """
    # Get the most recent data as a template for future predictions
    max_date = df_encoded["date"].max()

    # If target region is specified, filter the data
    if target_region:
        region_col = f"region_{target_region}"

        if region_col not in df_encoded.columns:
            available_regions = [
                col.replace("region_", "")
                for col in df_encoded.columns
                if col.startswith("region_")
            ]
            raise ValueError(
                f"Region '{target_region}' not found in dataset. Available countries: {available_regions}"
            )

        # Get all product types for the target region (not just from the most recent date)
        region_data = df_encoded[df_encoded[region_col] == 1].copy()

        if len(region_data) == 0:
            raise ValueError(f"No data available for region '{target_region}'")

        # Group by product type to get a representative sample for each product
        product_cols = [
            col for col in df_encoded.columns if col.startswith("item_type_")
        ]

        # Get unique product combinations in this region
        product_groups = []
        for _, group in region_data.groupby(product_cols):
            # Take the most recent record for each product type
            recent_product = group.loc[group["date"] == group["date"].max()].iloc[0:1]
            product_groups.append(recent_product)

        if product_groups:
            recent_data = pd.concat(product_groups, ignore_index=True)
        else:
            # Fallback to just the most recent data
            recent_data = region_data[
                region_data["date"] == region_data["date"].max()
            ].copy()
    else:
        # Use all recent data if no target region is specified
        recent_data = df_encoded[df_encoded["date"] == max_date].copy()

    # Add some randomness to make predictions more realistic
    # This simulates natural variation in sales
    random_seed = 42
    np.random.seed(random_seed)

    # Expand future data for each future date
    future_data_expanded = []
    for future_date in future_dates:
        date_data = recent_data.copy()

        # Update date-related features
        date_data["date"] = future_date
        date_data["year"] = future_date.year
        date_data["month"] = future_date.month
        date_data["day_of_month"] = future_date.day

        # Calculate other date features
        date_data["quarter"] = (date_data["month"] - 1) // 3 + 1
        date_data["month_of_quarter"] = ((date_data["month"] - 1) % 3) + 1
        date_data["day_of_week"] = future_date.weekday()
        date_data["week_of_year"] = future_date.isocalendar()[1]

        # Add small random variations to some numeric features to create day-to-day differences
        # This makes predictions more realistic by simulating natural variations
        for col in ["unit_price", "unit_cost"]:
            if col in date_data.columns:
                # Add up to ±3% random variation
                date_data[col] = date_data[col] * (
                    1 + np.random.uniform(-0.03, 0.03, size=len(date_data))
                )

        # Add day-of-week effect (e.g., weekends might have different sales patterns)
        weekday = future_date.weekday()
        weekend_factor = 1.15 if weekday >= 5 else 1.0  # 15% boost on weekends

        # Add seasonality effect based on month
        month = future_date.month
        # Summer months (June-August) might have different patterns
        summer_factor = 1.1 if 6 <= month <= 8 else 1.0

        # Combine factors - will be applied to predictions later
        date_data["seasonal_factor"] = weekend_factor * summer_factor

        future_data_expanded.append(date_data)

    future_data = pd.concat(future_data_expanded, ignore_index=True)

    # Get feature columns excluding the target, date and our custom factor
    feature_cols = [
        col
        for col in future_data.columns
        if col != "units_sold" and col != "date" and col != "seasonal_factor"
    ]

    # Make predictions
    predictions = model.predict(future_data[feature_cols])

    # Apply seasonal factors to add realistic variations
    predictions = predictions * future_data["seasonal_factor"].values

    # Add small random noise to predictions (±5%)
    predictions = predictions * (
        1 + np.random.uniform(-0.05, 0.05, size=len(predictions))
    )

    # Create results DataFrame
    results = pd.DataFrame(
        {
            "date": future_data["date"],
            "year": future_data["year"],
            "month": future_data["month"],
            "day": future_data["day_of_month"],
            "day_of_week": future_data["day_of_week"].map(
                {
                    0: "Monday",
                    1: "Tuesday",
                    2: "Wednesday",
                    3: "Thursday",
                    4: "Friday",
                    5: "Saturday",
                    6: "Sunday",
                }
            ),
            "item_type": future_data[
                [col for col in future_data.columns if col.startswith("item_type_")]
            ]
            .idxmax(axis=1)
            .str.replace("item_type_", ""),
            "product_category": future_data[
                [
                    col
                    for col in future_data.columns
                    if col.startswith("product_category_")
                ]
            ]
            .idxmax(axis=1)
            .str.replace("product_category_", ""),
            "predicted_units": predictions.round(0),
        }
    )

    # Add region information to the results
    if target_region:
        results["region"] = target_region
    else:
        # Extract region from one-hot encoded columns
        results["region"] = (
            future_data[
                [col for col in future_data.columns if col.startswith("region_")]
            ]
            .idxmax(axis=1)
            .str.replace("region_", "")
        )

    # Group by date, region, product type and calculate total predicted sales
    product_sales = (
        results.groupby(
            ["date", "region", "item_type", "product_category", "day_of_week"]
        )["predicted_units"]
        .sum()
        .reset_index()
    )
    product_sales = product_sales.sort_values(
        ["date", "region", "predicted_units"], ascending=[True, True, False]
    )

    # Also show summary by product across the entire period
    product_summary = (
        product_sales.groupby(["region", "item_type", "product_category"])[
            "predicted_units"
        ]
        .sum()
        .reset_index()
        .sort_values("predicted_units", ascending=False)
    )

    return product_sales

def plot_forescast(results, product_sales, region):
    # Get the top 5 item types from results
    top_5_items = results['item_type'].tolist()

    # Filter product_sales for only these items
    plot_data = product_sales[product_sales['item_type'].isin(top_5_items)].copy()

    # Ensure date is in datetime format
    plot_data['date'] = pd.to_datetime(plot_data['date'])

    # Create the plot
    plt.figure(figsize=(15, 8))

    # Create a line plot for each item type with weekly resampling
    for item in top_5_items:
        item_data = plot_data[plot_data['item_type'] == item]
        
        # Convert to weekly data by resampling and taking the mean
        weekly_data = (item_data.set_index('date')
                      .resample('W')['predicted_units']
                      .mean()
                      .reset_index())
        
        plt.plot(weekly_data['date'], 
                 weekly_data['predicted_units'], 
                 label=item, 
                 marker='o', 
                 markersize=4)

    plt.title(f'Weekly Predicted Sales - Top 5 Products in {region}')
    plt.xlabel('Date')
    plt.ylabel('Average Weekly Predicted Units')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def generate_marketing_prompts(base_text, results, reviews):
    """
    Generate marketing prompts for top products based on reviews and descriptions.
    
    Parameters:
    base_text (str): Template text for the marketing prompt
    results (DataFrame): DataFrame containing top items
    reviews (DataFrame): DataFrame containing product reviews and descriptions
    
    Returns:
    dict: Dictionary with item types as keys and their generated prompts as values
    """
    # Get items from results
    items = results['item_type'].values
    
    # Dictionary to store prompts
    prompts = {}
    
    # Generate prompts for each item
    for item in items:
        prompt = base_text.format(
            product_name=item,
            product_description=reviews[reviews['item_type'] == item]['product_description'].values[0],
            product_category=reviews[reviews['item_type'] == item]['product_category'].values[0],
            product_reviews=reviews[(reviews['item_type'] == item) & (reviews['score'] > 4)]['text'].values[:2]
        )
        prompts[item] = prompt
    
    return prompts

def print_marketing_prompts(prompts):
    """
    Print the generated marketing prompts with formatting.
    
    Parameters:
    prompts (dict): Dictionary of prompts generated by generate_marketing_prompts
    """
    for item, prompt in prompts.items():
        separator = "=" * 50
        print(f"\n{separator}")
        print(f"📝 GenAI prompt for: {item.upper()}")
        print(f"{separator}\n")
        print(prompt)

# Usage example:
# prompts = generate_marketing_prompts(base_text, results, reviews)
# print_marketing_prompts(prompts)