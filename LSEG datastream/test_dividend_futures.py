import json
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()

# Generate config from .env
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

ld.open_session(config_name=config_path)

end = datetime.now().strftime("%Y-%m-%d")
start_1y = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
start_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

# --- 1. Annual dividend futures continuations (1 year history) ---
print("Fetching annual dividend futures continuations...")
futures_rics = ["SDAc1", "SDAc2", "SDAc3", "SDAc4", "SDAc5", "SDAc6"]
futures_data = ld.get_history(
    universe=futures_rics,
    fields=["TRDPRC_1", "HIGH_1", "LOW_1", "ACVOL_UNS", "OPINT"],
    start=start_1y,
    end=end
)
print(f"Futures continuations: {futures_data.shape}")
print(futures_data.head(10))
futures_data.to_csv("sample_annual_div_futures.csv")

# --- 2. Quarterly dividend futures continuations ---
print("\nFetching quarterly dividend futures continuations...")
q_futures_rics = ["SDIc1", "SDIc2", "SDIc3"]
q_futures_data = ld.get_history(
    universe=q_futures_rics,
    fields=["TRDPRC_1", "HIGH_1", "LOW_1", "ACVOL_UNS", "OPINT"],
    start=start_1y,
    end=end
)
print(f"Quarterly futures: {q_futures_data.shape}")
print(q_futures_data.head(10))
q_futures_data.to_csv("sample_quarterly_div_futures.csv")

# --- 3. Sample options on annual dividend futures (Dec 2026 expiry) ---
print("\nFetching sample options (Dec 2026 expiry)...")
option_rics = [
    # Calls
    "1SDA78L26", "1SDA82L26", "1SDA84L26", "1SDA85L26", "1SDA86L26",
    # Puts
    "1SDA70X26", "1SDA75X26", "1SDA8075X26", "1SDA8825X26"
]
options_data = ld.get_history(
    universe=option_rics,
    fields=["TRDPRC_1", "HIGH_1", "LOW_1", "ACVOL_UNS", "OPINT", "SETTLE"],
    start=start_30d,
    end=end
)
print(f"Options (Dec 2026): {options_data.shape}")
print(options_data.head(20))
options_data.to_csv("sample_div_options_dec2026.csv")

# --- 4. Sample options Dec 2027 ---
print("\nFetching sample options (Dec 2027 expiry)...")
option_rics_27 = [
    "1SDA85L27", "1SDA87L27", "1SDA90L27",
    "1SDA64X27", "1SDA8725X27", "1SDA755X27"
]
options_data_27 = ld.get_history(
    universe=option_rics_27,
    fields=["TRDPRC_1", "HIGH_1", "LOW_1", "ACVOL_UNS", "OPINT", "SETTLE"],
    start=start_30d,
    end=end
)
print(f"Options (Dec 2027): {options_data_27.shape}")
print(options_data_27.head(20))
options_data_27.to_csv("sample_div_options_dec2027.csv")

print("\nAll data saved to CSV files.")

ld.close_session()
os.remove(config_path)
