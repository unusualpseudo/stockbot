import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession

# Function to collect financial data for a single stock
def get_metrics(ticker):
    stock = yf.Ticker(ticker)

    # Get the current stock price
    stock_price = stock.history(period="1mo")['Close'].iloc[-1]

    # Get EPS (Earnings per Share)
    eps = stock.info.get('trailingEps')

    # Get P/E ratio
    pe_ratio = None
    if eps:
        pe_ratio = round(stock_price / eps, 2)

    # Get P/B ratio
    book_value = stock.info.get('bookValue')
    pb_ratio = None
    if book_value:
        pb_ratio = round(stock_price / book_value, 2)

    # Get Return on Equity (ROE)
    roe = stock.info.get('returnOnEquity')
    if roe is not None:
        roe = round(roe * 100, 2)  # Convert to percentage

    # Return the collected data as a dictionary
    return {
        'Ticker': ticker,
        'Stock_Price': stock_price,  # Removed space
        'EPS': eps,
        'PE_Ratio': pe_ratio,        # Changed to use underscore
        'PB_Ratio': pb_ratio,        # Changed to use underscore
        'ROE_Percentage': roe,       # Removed special characters and used underscores
    }


# Function to collect data for multiple stocks
def collect_metrics(tickers):
    data = []
    for ticker in tickers:
        stock_data = get_metrics(ticker)
        data.append(stock_data)

    # Create a DataFrame to store the results
    df = pd.DataFrame(data)
    return df


# Example: Collect data for multiple stock tickers
tickers = ['AAPL', 'MSFT', 'GOOGL']  # Add more tickers as needed
financial_data = collect_metrics(tickers)

print(financial_data)

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Stock Data to Delta Lake") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark_df = spark.createDataFrame(financial_data)

delta_table_path = "../data/"

spark_df.write.format("delta").mode("overwrite").save(delta_table_path)

delta_df = spark.read.format("delta").load(delta_table_path)

delta_df.show()