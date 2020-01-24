#%%
from glob import glob
import json

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()


%matplotlib inline


def load_evnets():
    for filename in glob("../log/stats-*.json"):
        print(filename)
        with open(filename, "rt") as file:
            for line in file.readlines():
                yield json.loads(line.strip())


raw_events = list(load_evnets())
download_chunk_raw_events = [e for e in raw_events if e["event"] == "download_chunk"]
exchange_market_raw_events = [e for e in raw_events if e["event"] == "exchange_market"]

# %%
download_chunk_events = pd.DataFrame.from_dict(download_chunk_raw_events)
exchange_market_events = pd.DataFrame.from_dict(exchange_market_raw_events)

download_chunk_events.drop(["namespace", "event"], axis=1, inplace=True)
exchange_market_events.drop(["namespace", "key", "checksum", "event"], axis=1, inplace=True)

download_chunk_events["size"] = download_chunk_events["size"].apply(lambda s: s // (1024 * 1024))
# %%

download_chunk_events.groupby("to_peer")["size"].sum()

# %%
download_chunk_events.groupby("from_peer")["size"].sum()


# %%
exchange_market_events.groupby("to_peer").count()


# %%
