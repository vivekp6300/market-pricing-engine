# Market Pricing Engine

A reusable toolkit for fetching and storing daily market prices and ISIN-symbol mappings for Indian equities and mutual funds.

## Features
- Automatically maps ISINs to Yahoo Finance or AMFI symbols
- Fetches EQ prices from Yahoo Finance
- Fetches MF NAVs from AMFI
- Stores all prices in a daily historical CSV database

## Usage

```bash
python scripts/generate_symbol_map.py     # builds isin_symbol_map.csv
python scripts/fetch_prices.py            # builds price_history.csv
```
