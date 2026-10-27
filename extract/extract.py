"""Extract daily prices and company details from Alpha Vantage into JSON Lines files.
Run:  python extract.py
Needs: API_Key in a .env file
"""
import os, json, time
from datetime import date

import requests
from dotenv import load_dotenv

URL = "https://www.alphavantage.co/query"
TICKERS = ["IBM", "TSLA","NVDA","AAPL","AMD","GOOGL","MSFT","AMZN"] 
LANDING_DIR = "landing"
SLEEP_SECONDS = 13               

def check_reply(ticker, data):           
    for key in ("Error Message", "Note", "Information"):
        if key in data:
            raise ValueError(f"{ticker}: API refused: {data[key]}")

def flatten_daily(ticker, data):          
    flatten = []
    daily = data["Time Series (Daily)"]

    for day, value  in daily.items():
        
        flatten.append({
            'ticker': ticker,
            'Trade_date': day,
            'open': value['1. open'],
            'high': value['2. high'],
            'low': value['3. low'],
            'close': value['4. close'],
            'volume': value['5. volume'],
            
        })
    
    return flatten


def save_jsonl(rows, path):               
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def call_api(params, ticker, retries=3): 
    for attempt in range(1, retries+1): 
        try:  
            resp = requests.get(URL, params=params, timeout=20)
            resp.raise_for_status() 
            data = resp.json() 
        except requests.exceptions.RequestException as e:
            print(f"{ticker}: attempt {attempt} failed: {e}")
            time.sleep(SLEEP_SECONDS)
            continue
        check_reply(ticker, data)
        return data
    raise RuntimeError(f"{ticker}: failed after {retries} attempts")

def main():    
    load_dotenv()
    api_key = os.getenv("API_Key")
    if not api_key:
        raise SystemExit("API key missing: add API_Key to your .env file")

    os.makedirs(LANDING_DIR, exist_ok=True)
    today = date.today().isoformat()
    succeeded, failed = [], []

    for ticker in TICKERS:
        try:                                                        
            params = {"function": "TIME_SERIES_DAILY", "symbol": ticker, "apikey": api_key}
            data = call_api(params, ticker)
            rows = flatten_daily(ticker,data)
            save_jsonl(rows, f"{LANDING_DIR}/prices_{ticker}_{today}.jsonl")
            succeeded.append(ticker)                                
        except Exception as e:                                      
            print(f"{ticker}: FAILED: {e}")
            failed.append(ticker)


if __name__ == "__main__":
    main()