import pandas as pd
import json
x = [
    {
        'a': 'a',
        'b': 'b'
    },
    {
        'a': 'c',
        'b': 'd'
    },
    {
        'a': 'c',
        'b': 'd'
    }
]

df = pd.DataFrame(x)
print(df)
df.insert(0, 'New_ID', range(880, 880 + len(df)))
print(df)