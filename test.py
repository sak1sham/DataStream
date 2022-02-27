import awswrangler as wr

import pandas as pd

from dotenv import load_dotenv
load_dotenv() 

# initialize list of lists
data = [['tom', 10], ['nick', 15], ['juli', 14]]
 
# Create the pandas DataFrame
df = pd.DataFrame(data, columns = ['Name', 'Age'])
file_ = 's3://migration-service-temp/temp/abc.snappy.parquet'

wr.s3.to_parquet(
    df = df,
    path = file_,
    compression = 'snappy',
)