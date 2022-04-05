import pandas as pd  
import awswrangler as wr
from dotenv import load_dotenv
load_dotenv()


#data = {'Name': ['Tom3', 'Joseph3', 'Krish3', 'John3', 'Tom4', 'Joseph4', 'Krish4', 'John4'], 'Age': [120, 121, 119, 118, 140, 142, 138, 136], 'Gender': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'F']}  
data = {'Name': ['Tom5', 'Krish5', 'Tom6', 'Krish6'], 'Age': [120, 119, 140, 138], 'Gender': ['M', 'M', 'M', 'M']}  
df_u = pd.DataFrame(data)
df_u.drop('Gender', axis=1, inplace=True)
df_u.columns = df_u.columns.str.lower()
print(df_u, end='\n\n')

def df_update_records(df = pd.DataFrame({}), df_u = pd.DataFrame({}), primary_key: str = None):
    common = df_u[df_u[primary_key].isin(df[primary_key])]
    if(common.shape[0]):
        uncommon = df_u[~df_u[primary_key].isin(df[primary_key])]
        final_df = pd.concat([df, common]).drop_duplicates(subset=[primary_key], keep='last')
        final_df.reset_index(drop=True, inplace=True)
        return final_df, True, uncommon
    else:
        return df, False, df_u

df = wr.s3.read_parquet('s3://data-migration-server/temp/gender=M/67c83a643aa84f13a20b8f5da0a6fa08.snappy.parquet')
print(df, end='\n\n')

o = df_update_records(df, df_u, 'age')
print(o[0], end='\n\n')
print(o[1], end='\n\n')
print(o[2], end='\n\n')

wr.s3.to_parquet(
    df = o[0],
    path = 's3://data-migration-server/temp/gender=M/67c83a643aa84f13a20b8f5da0a6fa08.snappy.parquet',
    compression = 'snappy'
)

# wr.s3.to_parquet(
#     df = df,
#     path = 's3://data-migration-server/temp',
#     compression='snappy',
#     mode = 'append',
#     database = 'test_db',
#     table = 'testing',
#     dtype = {'Name': 'string', 'Age': 'bigint', 'Gender': 'string'},
#     description = 'self.description',
#     dataset = True,
#     partition_cols = ['Gender'],
#     schema_evolution = True,
# )
