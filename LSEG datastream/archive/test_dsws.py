import DatastreamPy as DSWS
from dotenv import load_dotenv
import os

load_dotenv()

ds = DSWS.DataClient(None, os.getenv("DSWS_USERNAME"), os.getenv("DSWS_PASSWORD"))

# Snapshot request for AAPL price
data = ds.get_data(tickers='@AAPL', fields=['P', 'MV', 'NAME'], kind=0)
print(data)
