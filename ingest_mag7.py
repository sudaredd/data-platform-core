import requests
import json
from datetime import datetime, timedelta

# MAG 7 Tickers and some fake stock prices for Jan 2025
mag7 = {
    "AAPL": 230.50,
    "MSFT": 415.20,
    "GOOGL": 190.15,
    "AMZN": 205.80,
    "NVDA": 135.25,
    "META": 580.40,
    "TSLA": 250.75
}

base_url = "http://localhost:8081/api/ingest/batch"

def ingest_ticker_data(ticker, base_price):
    print(f"Ingesting data for {ticker}...")
    
    # Ingest 7 days of data for Jan 2025
    data_points = []
    for i in range(7):
        date = (datetime(2025, 1, 20) + timedelta(days=i)).strftime("%Y-%m-%d")
        price = base_price + (i * 2.5) # simple linear growth for nice charts
        
        point = {
            "tenant_id": ticker,
            "instrument_id": ticker,
            "field_id": "CLOSE",
            "period_date": date,
            "data": {
                "value": round(price, 2),
                "report_time": f"{date}T16:00:00Z"
            }
        }
        data_points.append(point)
    
    payload = {
        "tenantId": ticker,
        "periodicity": "DAILY",
        "data": data_points
    }
    
    try:
        response = requests.post(base_url, json=payload)
        if response.status_code == 200:
            print(f"Successfully ingested data for {ticker}")
        else:
            print(f"Failed to ingest for {ticker}: {response.text}")
    except Exception as e:
        print(f"Error ingesting for {ticker}: {str(e)}")

if __name__ == "__main__":
    for ticker, price in mag7.items():
        ingest_ticker_data(ticker, price)
