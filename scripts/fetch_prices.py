from pricing_engine.price_fetcher import PriceFetcher

if __name__ == "__main__":
    fetcher = PriceFetcher()
    fetcher.update_price_history()
