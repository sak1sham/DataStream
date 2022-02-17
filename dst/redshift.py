import redshift_connector

conn = redshift_connector.connect(
    host = "redshift-cluster-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com",
    database = 'dev',
    user = 'saksham@citymall.live',
    password = 'CitymallDevAdmin123'
)

print(conn)
print("Hi")
