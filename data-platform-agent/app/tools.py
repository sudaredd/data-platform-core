import os
import requests
from langchain.tools import tool
import json
from dotenv import load_dotenv

load_dotenv()

@tool
def fetch_daily_data(ticker: str, start_date: str, end_date: str, tenant_id: str = "DEFAULT") -> str:
    """
    Fetches daily pricing data for a given ticker and date range.
    
    Args:
        ticker (str): The instrument ID (e.g., IBM).
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.
        tenant_id (str): The tenant ID context for the query. Defaults to "DEFAULT" if not provided, 
                        but usually should be passed from the agent's context.

    Returns:
        str: JSON string response from the Java Query Service.
    """
    print(f"DEBUG: fetch_daily_data called with ticker={ticker}, start={start_date}, end={end_date}, tenant={tenant_id}")
    
    url = f"http://localhost:8082/api/query/{tenant_id}/DAILY"
    payload = {
        "instrument_id": ticker,
        "start_date": start_date,
        "end_date": end_date
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return json.dumps(response.json())
    except requests.exceptions.RequestException as e:
        return f"Error fetching daily data: {str(e)}"

@tool
def fetch_realtime_data(ticker: str, interval: str = "5min") -> str:
    """
    Fetches real-time (intraday) pricing data for a given ticker using Alpha Vantage.
    
    Args:
        ticker (str): The stock ticker symbol (e.g., IBM).
        interval (str): The time interval between data points. 
                       Options: '1min', '5min', '15min', '30min', '60min'. Default is '5min'.

    Returns:
        str: A summary of recent intraday prices.
    """
    print(f"DEBUG: fetch_realtime_data called for {ticker} at {interval} intervals")
    
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&apikey={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Alpha Vantage returns data in "Time Series (Xmin)" key
        time_series_key = f"Time Series ({interval})"
        if time_series_key not in data:
            return f"Could not find intraday data for {ticker}. Alpha Vantage Note: {data.get('Note', data.get('Error Message', 'Unknown error'))}"
        
        # Take the most recent 10 data points for brevity
        series = data[time_series_key]
        recent_points = list(series.items())[:10]
        
        formatted_results = [f"Real-time data for {ticker} ({interval} intervals):"]
        for timestamp, values in recent_points:
            price = values["4. close"]
            formatted_results.append(f"{timestamp}: ${price}")
            
        return "\n".join(formatted_results)
        
    except Exception as e:
        return f"Error fetching real-time data: {str(e)}"
