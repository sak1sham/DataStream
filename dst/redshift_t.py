import awswrangler as wr
import pandas as pd
from datetime import date
import redshift_connector

def redshift_test():
    conn = redshift_connector.connect(
            host = "redshift-cluster-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com",
            database = 'dev',
            user = 'admin-redshift',
            password = 'CitymallDevAdmin123'
        )
    df = pd.DataFrame({
        "id": [1, 2],
        "value": ["foo", "boo"],
        "date": [date(2020, 1, 1), date(2020, 1, 2)]
    })
    wr.redshift.copy(
        df = df,
        con = conn,
        path = "s3://data-migration-server/redshift/",
        schema = 'migration_service',
        table = 'my_table',
        mode = 'append',
    )
    print(wr.redshift.read_sql_table(table="my_table", schema="migration_service", con=conn))
