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
    }
]

#print(pd.DataFrame(x))
x = json.dumps(x)
print(json.loads(x))