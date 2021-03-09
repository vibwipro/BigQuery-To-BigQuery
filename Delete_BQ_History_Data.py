from dateutil.parser import parse
import datetime, sys
from google.cloud import bigquery
from datetime import datetime, timedelta

Dataset_Table_name = sys.argv[1]
coll_name = sys.argv[2]
date_ip = sys.argv[3]
no_day = sys.argv[4]

arch_date = (datetime.strptime(date_ip, '%Y-%m-%d') - timedelta(int(no_day))).strftime('%Y-%m-%d')

stream_query = """DELETE FROM `PROJ.""" + Dataset_Table_name + """` WHERE """ + coll_name + """ < '""" + arch_date + """'"""

print (stream_query)

stream_client = bigquery.Client()
stream_Q = stream_client.query(stream_query)
stream_data_df = stream_Q.to_dataframe()

