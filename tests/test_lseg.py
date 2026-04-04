import json
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()

# Generate config file from .env credentials
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

config_path = os.path.join(os.path.dirname(__file__), "lseg-data.config.json")
with open(config_path, "w") as f:
    json.dump(config, f, indent=4)

# Open session using generated config
session = ld.open_session(config_name=config_path)

# Get AAPL daily prices for last 5 days
data = ld.get_history(
    universe="AAPL.O",
    fields=["BID", "ASK", "TRDPRC_1"],
    start=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
    end=datetime.now().strftime("%Y-%m-%d")
)

print(data)
ld.close_session()

# Clean up config file (contains credentials)
os.remove(config_path)
