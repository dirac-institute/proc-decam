from .query import cli_query
from .api import search
import pandas as pd
import json
import io

query = cli_query()
keys = ["EXPNUM", "OBJECT", "dateobs_min", "exposure", "md5sum"]
query['outfields'] = keys
results = search(query=query)

with io.StringIO() as f:
    f.write(json.dumps(results))
    f.seek(0)
    print(pd.read_json(f)[keys].to_csv(index=False))

