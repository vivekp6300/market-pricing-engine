import pandas as pd
import yfinance as yf
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class PriceFetcher:
    def __init__(self, equity_map_path="data/master_equity_map.csv", output_path="data/price_history.csv", missing_path="data/missing_prices.csv"):
        self.equity_map_path = Path(equity_map_path)
        self.output_path = Path(output_path)
        self.missing_path = Path(missing_path)
        self.today = datetime.today().strftime("%Y-%m-%d")

        self.isin_map = pd.read_csv(self.equity_map_path)

        if self.output_path.exists():
            self.price_history = pd.read_csv(self.output_path)
        else:
            self.price_history = pd.DataFrame(columns=["Date", "ISIN", "Symbol", "Price", "Source"])

        if self.missing_path.exists():
            self.missing_log = pd.read_csv(self.missing_path)
            self.known_missing = set(zip(self.missing_log["ISIN"], self.missing_log["Symbol"]))
        else:
            self.missing_log = pd.DataFrame(columns=["Date", "ISIN", "Symbol", "Reason"])
            self.known_missing = set()

    def fetch_individual_price(self, symbol):
        try:
            hist = yf.Ticker(symbol).history(period="1d")
            if hist.empty:
                return None
            return float(hist['Close'].iloc[-1])
        except Exception as e:
            logger.warning(f"Exception fetching individual price for {symbol}: {e}")
            return None

    def update_price_history(self):
        all_prices = []
        missing = []

        active_rows = self.isin_map[~self.isin_map.apply(lambda row: (row["ISIN"], row["Symbol"]) in self.known_missing, axis=1)]
        symbols = active_rows["Symbol"].unique().tolist()

        # Step 1: Bulk fetch current prices
        try:
            logger.info(f"Attempting bulk download for {len(symbols)} symbols...")
            bulk_df = yf.download(symbols, period="1d", group_by='ticker', threads=True, progress=False)
        except Exception as e:
            logger.error(f"Bulk download failed: {e}")
            bulk_df = pd.DataFrame()

        for _, row in active_rows.iterrows():
            isin = row["ISIN"]
            symbol = row["Symbol"]
            price = None

            try:
                if symbol in bulk_df.columns.levels[0]:
                    price = bulk_df[symbol]['Close'].iloc[-1]
            except Exception as e:
                logger.debug(f"Error accessing bulk data for {symbol}: {e}")

            if price is None:
                price = self.fetch_individual_price(symbol)

            if price is not None:
                all_prices.append({
                    "Date": self.today,
                    "ISIN": isin,
                    "Symbol": symbol,
                    "Price": price,
                    "Source": "yfinance"
                })
            else:
                missing.append({
                    "Date": self.today,
                    "ISIN": isin,
                    "Symbol": symbol,
                    "Reason": "No data from Yahoo"
                })
                self.known_missing.add((isin, symbol))

        if all_prices:
            df_new = pd.DataFrame(all_prices)
            self.price_history = pd.concat([self.price_history, df_new], ignore_index=True)
            self.price_history.drop_duplicates(subset=["Date", "ISIN"], keep="last", inplace=True)
            self.price_history.to_csv(self.output_path, index=False)
            logger.info(f"Price history updated with {len(all_prices)} records.")

        if missing:
            df_miss = pd.DataFrame(missing)
            self.missing_log = pd.concat([self.missing_log, df_miss], ignore_index=True)
            self.missing_log.drop_duplicates(subset=["Date", "ISIN"], keep="last", inplace=True)
            self.missing_log.to_csv(self.missing_path, index=False)
            logger.info(f"Logged {len(missing)} missing price entries.")

        if not all_prices and not missing:
            logger.info("No new price data available.")
