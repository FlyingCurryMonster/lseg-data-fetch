"""
Get current stock prices for NVDA, AMD, and SPX to determine ATM strikes
for April 2026 option selection.
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()

config = {
    "sessions": {
        "default": "platform.rdp",
        "platform": {
            "rdp": {
                "app-key": os.getenv("DSWS_APPKEY"),
                "username": os.getenv("DSWS_USERNAME"),
                "password": os.getenv("DSWS_PASSWORD"),
                "signon_control": True
            }
        }
    }
}

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lseg-data.config.json")
with open(config_path, "w") as f:
    json.dump(config, f, indent=4)

session = ld.open_session(config_name=config_path)

# Get current/recent prices for our three underlyings
# NVDA.O = NVDA on Nasdaq, AMD.O = AMD on Nasdaq, .SPX = S&P 500 index
rics = ["NVDA.O", "AMD.O", ".SPX"]

data = ld.get_history(
    universe=rics,
    fields=["TRDPRC_1", "BID", "ASK", "HIGH_1", "LOW_1"],
    start=(datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d"),
    end=datetime.now().strftime("%Y-%m-%d")
)

print("=" * 60)
print("CURRENT PRICES FOR ATM STRIKE SELECTION")
print("=" * 60)
print(data)
print()

# Show the latest price for each
for ric in rics:
    try:
        if isinstance(data.columns, type(data.columns)) and hasattr(data, 'xs'):
            latest = data[ric].dropna(subset=["TRDPRC_1"]).iloc[-1]
        else:
            latest = data.dropna(subset=["TRDPRC_1"]).iloc[-1]
        print(f"{ric}: Last = {latest['TRDPRC_1']}")
    except Exception as e:
        print(f"{ric}: Could not extract latest price - {e}")

ld.close_session()
os.remove(config_path)
