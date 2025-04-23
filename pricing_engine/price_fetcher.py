import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from pathlib import Path


class PriceFetcher:
    def __init__(self, isin_map_path="data/isin_symbol_map.csv", output_path="data/price_history.csv"):
        self.isin_map_path = Path(isin_map_path)
        self.output_path = Path(output_path)
        self.today = datetime.today().strftime("%Y-%m-%d")

        self.isin_map = pd.read_csv(self.isin_map_path) if self.isin_map_path.exists() else pd.DataFrame(columns=["ISIN", "Symbol", "Type"])
        self.price_history = pd.read_csv(self.output_path) if self.output_path.exists() else pd.DataFrame(columns=["Date", "ISIN", "Symbol", "Price", "Source"])

    def fetch_yfinance_price(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            price = ticker.history(period="1d")["Close"].iloc[-1]
            return float(price)
        except Exception as e:
            print(f"[WARN] Failed to fetch price for {symbol} from yfinance: {e}")
            return None

    def fetch_amfi_prices(self):
        try:
            url = "https://www.amfiindia.com/spages/NAVAll.txt"
            r = requests.get(url, timeout=10)
            records = []
            for line in r.text.splitlines():
                parts = line.split(';')
                if len(parts) >= 5:
                    amfi_code, isin, _, _, nav = parts[:5]
                    try:
                        records.append({
                            "ISIN": isin.strip(),
                            "Price": float(nav.strip()),
                            "Source": "amfi",
                            "Symbol": amfi_code.strip(),
                            "Date": self.today
                        })
                    except:
                        continue
            return pd.DataFrame(records)
        except Exception as e:
            print(f"[WARN] Failed to fetch AMFI NAVs: {e}")
            return pd.DataFrame()

    def update_price_history(self):
        all_prices = []
        amfi_prices = self.fetch_amfi_prices()

        for _, row in self.isin_map.iterrows():
            isin, symbol, typ = row["ISIN"], row["Symbol"], row["Type"]
            if typ == "EQ":
                price = self.fetch_yfinance_price(symbol)
                if price:
                    all_prices.append({"Date": self.today, "ISIN": isin, "Symbol": symbol, "Price": price, "Source": "yfinance"})
            elif typ == "MF":
                match = amfi_prices[amfi_prices["ISIN"] == isin]
                if not match.empty:
                    all_prices.append(match.iloc[0].to_dict())

        if all_prices:
            df_new = pd.DataFrame(all_prices)
            self.price_history = pd.concat([self.price_history, df_new], ignore_index=True)
            self.price_history.drop_duplicates(subset=["Date", "ISIN"], keep="last", inplace=True)
            self.price_history.to_csv(self.output_path, index=False)
            print(f"[INFO] Price history updated with {len(all_prices)} records.")
        else:
            print("[INFO] No new price data available.")
