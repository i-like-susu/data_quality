import numpy as np
import pandas as pd
from data_quality_verify import write_excel
from pyhive import hive


conn = hive.Connection(host='10.120.7.199', port=18000, username='root', database='zxvmax')
cursor = conn.cursor()
sql = '''
select mme_ip_add, times, fail, msisdn_fill, imsi_fill, msisdn_fill*100/times as msisdn_fill_rate, imsi_fill*100/times as imsi_fill_rate
from(
select mme_ip_add, count(1) times, 
sum(case when procedure_status = 3 then 1 else 0 end) fail, sum(case when msisdn != '' then 1 else 0 end)
msisdn_fill, sum(case when imsi != '' then 1 else 0 end) imsi_fill
from fact_pscp_s1mme where reportdate = '2021-12-19' and reporthour = 01 
and procedure_type not in (4,11,22, 23, 24, 25, 26, 27, 28, 53, 54, 55, 56) group by mme_ip_add
) as a
'''
cursor.execute(sql)
result = cursor.fetchall()
print(result)
cursor.close()
conn.close()



