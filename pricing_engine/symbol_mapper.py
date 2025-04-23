import pandas as pd
import yfinance as yf
import requests
import re
from pathlib import Path


def validate_yfinance_symbol(symbol):
    try:
        info = yf.Ticker(symbol).info
        return "shortName" in info
    except:
        return False


def load_amfi_nav_table():
    try:
        url = "https://www.amfiindia.com/spages/NAVAll.txt"
        r = requests.get(url, timeout=10)
        records = []
        for line in r.text.splitlines():
            parts = line.split(';')
            if len(parts) >= 5:
                amfi_code, isin, _, name = parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()
                records.append({"ISIN": isin, "Symbol": amfi_code, "Name": name})
        return pd.DataFrame(records)
    except Exception as e:
        print(f"[WARN] Failed to load AMFI data: {e}")
        return pd.DataFrame()


def generate_isin_symbol_map(isin_master_path="data/isin_master.csv", output_path="data/isin_symbol_map.csv"):
    isin_master = pd.read_csv(isin_master_path)
    isin_master.columns = ["ISIN", "Name", "liq_status", "Type"]
    isin_master.dropna(subset=["ISIN"], inplace=True)
    amfi_df = load_amfi_nav_table()
    mapped = []

    for _, row in isin_master.iterrows():
        isin, name, typ = row["ISIN"], row["Name"], row["Type"]
        symbol = ""

        if typ == "EQ":
            probable = re.split(r'[ #(/]', name)[0].upper()
            yfin_sym = probable + ".NS"
            if validate_yfinance_symbol(yfin_sym):
                symbol = yfin_sym
        elif typ == "MF":
            match = amfi_df[amfi_df["ISIN"] == isin]
            if not match.empty:
                symbol = match.iloc[0]["Symbol"]

        if symbol:
            mapped.append({"ISIN": isin, "Symbol": symbol, "Type": typ})

    df_map = pd.DataFrame(mapped)
    df_map.to_csv(output_path, index=False)
    print(f"[INFO] Generated {len(df_map)} ISIN-symbol mappings in {output_path}")
