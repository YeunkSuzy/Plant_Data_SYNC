import iotdb
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import random
import time
import pymysql
from parameter_get import *
from connection import *
from config import *

#############  database connection
# Connect to IoTDB
print("Connecting to IoTDB...")
session = get_session_connection()
print(f"IoTDB connected, timezone: {session.get_time_zone()}")
# Connect to MySQL
print("Connecting to MySQL...")
mysql_cursor, mysql_db = get_mysql_connection()
print("MySQL connected")

############# create timeseries
for table_name in ("cryogenic_data", "quenchsc_data", "tds_data", "technique_data", "vacuum_data"):

    # 查询 MySQL 数据
    mysql_query = f"SELECT * FROM {table_name} limit 1 "
    mysql_cursor.execute(mysql_query)

    # 获取各项参数列表,并将其用sclcing_list函数以part_size分割
    part_size = 60
    measurements_lsts, data_type_lsts = parameter_choice(table_name, mysql_cursor)
    measurements_lst = slicing_list(measurements_lsts,part_size)
    data_type_lst = slicing_list(data_type_lsts,part_size)
    encoding_lst = [TSEncoding.PLAIN for _ in range(part_size)]
    compressor_lst = [Compressor.SNAPPY for _ in range(part_size)]
    devices = devices_get(measurements_lsts, part_size, table_name)

    #开始创建IoTDB时间序列
    for i in range(len(devices)):
        for j in range(len(measurements_lst[i])):
            if measurements_lst[i][j] == "time":
                 measurements_lst[i][j] = "curr_time"
            session.create_time_series(
                 f"root.{table_name}.{devices[i]}.{measurements_lst[i][j]}", data_type_lst[i][j], TSEncoding.PLAIN, Compressor.SNAPPY
        )
    print(f"{table_name} Create Completed!")


############### connection disclose
close_mysql_connection(mysql_cursor, mysql_db)
session.close()