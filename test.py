import awswrangler as wr   
import pytz
ist_tz = pytz.timezone('Asia/Kolkata')
import datetime

def utc_to_local(utc_dt, tz_):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(tz_)
    return tz_.normalize(local_dt)

def expire(expiry, tz) -> None:
    today_ = datetime.datetime.utcnow()
    if(tz):
        today_ = utc_to_local(today_, tz)
    days = 0
    hours = 0
    if('days' in expiry.keys()):
        days = expiry['days']
    if('hours' in expiry.keys()):
        hours = expiry['hours']
    delete_before_date = today_ - datetime.timedelta(days=days, hours=hours)
    wr.s3.delete_objects(
        path = "s3://migration-service-temp/Athena_query_results/",
        last_modified_end = delete_before_date
    )

timep = {
    'days': 20,
}

expire(timep, ist_tz)

