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


def load_yahoo_equity_map(path):
    df = pd.read_csv(path)
    df = df.rename(columns={" ISIN NUMBER": "ISIN", "Yahoo_Equivalent_Code": "Yahoo_Symbol"})
    df["ISIN"] = df["ISIN"].str.strip()
    df["Yahoo_Symbol"] = df["Yahoo_Symbol"].str.strip().str.strip("'")
    return df[["ISIN", "Yahoo_Symbol"]]


def load_existing_map(path):
    if Path(path).exists():
        return pd.read_csv(path)
    else:
        return pd.DataFrame(columns=["ISIN", "Symbol", "Type"])


def generate_isin_symbol_map(
    isin_master_path="data/isin_master.csv",
    output_path="data/isin_symbol_map.csv",
    yahoo_equity_path="data/EQUITY_L.csv"
):
    isin_master = pd.read_csv(isin_master_path)
    isin_master.columns = ["ISIN", "Name", "liq_status", "Type"]
    isin_master = isin_master.dropna(subset=["ISIN"])

    # Load prior known mappings and Yahoo reference
    equity_ref = load_yahoo_equity_map(yahoo_equity_path)
    prior_map = load_existing_map(output_path)

    # Detect new ISINs
    new_isins = isin_master[~isin_master["ISIN"].isin(prior_map["ISIN"])]

    # Try matching with Yahoo equity reference file
    auto_matched = pd.merge(new_isins, equity_ref, on="ISIN", how="inner")
    auto_matched = auto_matched[["ISIN", "Yahoo_Symbol", "Type"]].rename(columns={"Yahoo_Symbol": "Symbol"})

    # Combine and deduplicate
    combined = pd.concat([prior_map, auto_matched], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ISIN"], keep="last")
    combined.to_csv(output_path, index=False)

    print(f"[INFO] Updated ISIN-symbol map with {len(auto_matched)} new mappings. Total: {len(combined)}")
