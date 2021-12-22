import re

import pymysql as ps
import pandas as pd
import os
from pyhive import hive
from gbase_sql import gbase_sql_list
from spark_sql import spark_sql_list
from concurrent.futures import ThreadPoolExecutor, as_completed

# 如果在gbase里面核查数据质量，修改connect函数里面的host,usr,password,修改为现场的
def execute_gbase_sql(SQL):
    # 处理sql语句的函数,返回处理后的数据,以data_frame格式返回
    conn = ps.connect(host='10.245.146.71', port=5258, user='root', password='ZXvmax_2017',
                      database='zxvmax', charset='utf8', autocommit=True)

    cursor = conn.cursor()

    cursor.execute(SQL)
    data = cursor.fetchall()
    data = list(data)

    # 获取列名
    col_tuple = cursor.description
    col_name = [col[0] for col in col_tuple]
    dataframe = pd.DataFrame(data, columns=col_name)
    dataframe.loc[''] = ''
    return dataframe

# 如果在sparksql里面核查数据质量，修改Connection里面的host,username和database,修改为现场的
def execute_spark_sql(SQL):
    # 处理sql语句的函数,返回处理后的数据,以data_frame格式返回
    conn = hive.Connection(host='10.120.7.199', port=18000, username='root', database='zxvmax')

    cursor = conn.cursor()

    cursor.execute(SQL)
    data = cursor.fetchall()
    data = list(data)

    # 获取列名
    col_tuple = cursor.description
    col_name = [col[0] for col in col_tuple]
    dataframe = pd.DataFrame(data, columns=col_name)
    dataframe.loc[''] = ''
    return dataframe


def write_excel(dataframe):
    sheet_name = 'data_quality'

    excel_path = os.path.join(os.path.expanduser("~"),
                              'Desktop') + '\\' + '数据质量结果' + '.csv'
    dataframe.to_csv(excel_path, index=None, header=True, mode='a')

    print('----------数据成功写入excel----------')


def threadpool_execute_data(sql_list, thread_num):
    '''
            利用线程池来处理数据，提高数据提取的效率
            sql_list: 待处理的sql列表
            thread_num: 线程池中线程的个数
    '''
    executor = ThreadPoolExecutor(thread_num)
    gene = generate_title()
    for data in executor.map(execute_spark_sql, sql_list):
        write_excel(next(gene))
        write_excel(data)


# 用于sqarksql的正则表达式
def replace_date_spark(date, hour, my_sql):
    re.sub('\d+-\d+-\d+', date, my_sql)
    res = re.sub('(reporthour = )(\d+)', lambda x: x.group(1) + hour, my_sql)
    return res


# 用于gbase的正则表达式
def replace_date_gbase(date, hour, my_sql):
    tmp = re.sub('\d{8}', date, my_sql)
    res = re.sub('(hour\(start_time\) = )(\d+)', lambda x: x.group(1) + hour, tmp)
    return res


def generate_title():
    title_name = ['s1mme', 's11_mme_dimension',
                  's11_sgw_dimension', 's6a', 'http']
    for item in title_name:
        df_dict = pd.DataFrame({item: []}, index=None)
        yield df_dict


if __name__ == '__main__':
    # 在gbase里面取数据
    # sql_list = [replace_date_gbase('20211220', '09', mysql) for mysql in gbase_sql_list]

    # 在sparksql里面取数据，需要修改日期和小时，日期写成20210101格式，小时写成01格式
    sql_list = [replace_date_spark('20211220', '09', mysql) for mysql in spark_sql_list]
    print('开始从数据库里面取数据执行')
    threadpool_execute_data(sql_list, 2)
    print('执行结束')
