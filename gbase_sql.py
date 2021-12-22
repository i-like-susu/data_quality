# s1mme完整回填率
s1mme = '''
select mme_ip_add, times, fail, msisdn_fill, imsi_fill, msisdn_fill*100/times as msisdn_fill_rate, imsi_fill*100/times as imsi_fill_rate 
from(
select mme_ip_add, count(1) times, 
sum(case when procedure_status = 3 then 1 else 0 end) fail, sum(case when msisdn != '' then 1 else 0 end)
msisdn_fill, sum(case when imsi != '' then 1 else 0 end) imsi_fill 
from db_fact_pscp_s1mme_20211212 where hour(start_time) = 09 
and procedure_type not in (4,11,22, 23, 24, 25, 26, 27, 28, 53, 54, 55, 56) group by mme_ip_add
) as a
'''

# s11完整回填率，以MME进行统计
s11_mme_dimension = '''
select mme_address, times, fail, msisdn_fill, imsi_fill, msisdn_fill*100/times as msisdn_fill_rate, imsi_fill*100/times as imsi_fill_rate
from(
select mme_address, count(1) times, sum(case when procedure_status = 3 then 1 else 0 end) fail,  
sum(case when msisdn != '' then 1 else 0 end) msisdn_fill, sum(case when imsi != '' then 1 else 0 end) imsi_fill 
from db_fact_pscp_s11_20210101 where hour(start_time) = 10  and procedure_type!=8 group by mme_address
) as a

'''


# s11完整回填率，以SGW统计
s11_sgw_dimension = '''
select sgw_address, times, fail, msisdn_fill, imsi_fill, msisdn_fill*100/times as msisdn_fill_rate, imsi_fill*100/times as imsi_fill_rate
from(
select sgw_address, count(1) times, sum(case when procedure_status = 3 then 1 else 0 end) fail,sum(case when msisdn != '' then 1 else 0 end) msisdn_fill, 
sum(case when imsi != '' then 1 else 0 end) imsi_fill 
from db_fact_pscp_s11_20210101 where hour(start_time) = 10 and procedure_type!=8 group by sgw_address
) as a

'''


# s6a完整回填率
s6a = '''
select mme_address, times, fail, msisdn_fill, imsi_fill, msisdn_fill*100/times as msisdn_fill_rate, imsi_fill*100/times as imsi_fill_rate
from(
select mme_address, count(1) times, sum(case when procedure_status = 3 then 1 else 0 end) fail,
sum(case when msisdn != '' then 1 else 0 end) msisdn_fill,sum(case when imsi != '' then 1 else 0 end) imsi_fill 
from db_fact_pscp_s6a_20210101 where hour(start_time) = 18  group by mme_address
) as a
'''

# http的回填率
http = '''
select source_ip,count(1) times,sum(case when ((imsi!='') and (imsi not like 'FFFF%')) then 1 else 0 end)*100/count(*) as imsi_ok 
from db_fact_psup_http_20210101 where hour(start_time) = 10 and interface=11 group by source_ip
'''

# sql_list = [s1mme]
gbase_sql_list = [s1mme, s11_mme_dimension, s11_sgw_dimension, s6a, http]