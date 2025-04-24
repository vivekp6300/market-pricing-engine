import requests
import pandas as pd
import logging
from datetime import datetime
from io import StringIO
import traceback

# Setup logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUTPUT_CSV = "data/mf_nav_history.csv"


def fetch_and_parse_amfi_nav():
    try:
        logger.info("Fetching NAVAll.txt from AMFI...")
        response = requests.get(NAV_URL)
        response.encoding = "utf-8"
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: Failed to download NAVAll.txt")

        lines = response.text.splitlines()

        logger.info("First 30 lines from AMFI file:")
        for i, line in enumerate(lines[:30]):
            logger.info(f"{i+1:02d}: {line}")

        header_line = next((i for i, line in enumerate(lines) if "Scheme Code" in line), None)
        if header_line is None:
            raise Exception("Failed to locate data header in NAVAll.txt")

        nav_data = "\n".join(lines[header_line:])
        df = pd.read_csv(StringIO(nav_data), sep=";", engine="python")

        if df.empty:
            raise Exception("Parsed NAV dataframe is empty.")

        df = df[["ISIN Div Payout/ ISIN Growth", "Scheme Name", "Net Asset Value", "Date"]]
        df.columns = ["ISIN", "Fund_Name", "NAV", "Date"]
        df = df.dropna(subset=["ISIN", "NAV"])

        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True).dt.strftime("%Y-%m-%d")
        df["NAV"] = pd.to_numeric(df["NAV"], errors="coerce")
        df = df[df["NAV"].notna()]

        latest_date = df["Date"].max()
        df_today = df[df["Date"] == latest_date]
        logger.info(f"Fetched {len(df_today)} mutual fund NAVs for today.")

        try:
            existing = pd.read_csv(OUTPUT_CSV)
            combined = pd.concat([existing, df_today], ignore_index=True)
            combined = combined.drop_duplicates(subset=["Date", "ISIN"], keep="last")
        except FileNotFoundError:
            combined = df_today

        combined.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"NAV history updated: {OUTPUT_CSV}")

    except Exception as e:
        logger.error("Failed to fetch NAVs:")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    fetch_and_parse_amfi_nav()
