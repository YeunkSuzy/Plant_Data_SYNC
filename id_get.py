import pymysql
import os
import configparser

############# config get
config = configparser.ConfigParser(interpolation=None)
config.read('sync_config.config')
# 读取配置
sync_max = int(config['sync config']['sync_max'])
sync_count = int(config['sync config']['sync_count'])
sync_interval = int(config['sync config']['sync_interval'])

############# File_Storage_id
# 为指定表获取last_id（存储在last_ids文件夹中）
def get_last_id_for_table(table_name):
    # 确保last_ids文件夹存在
    last_ids_dir = "last_ids"
    if not os.path.exists(last_ids_dir):
        os.makedirs(last_ids_dir)
    
    table_id_file = os.path.join(last_ids_dir, f'{table_name}_last_id.txt')
    try:
        # 打开表特定的id存储文件
        with open(table_id_file, mode='r') as file_object:
            # 尝试从文件中读取最后一个处理的ID
            content = file_object.read()
            if not content:
                return 0
            last_id = int(content)
    except FileNotFoundError:
        # 如果文件不存在，则返回0作为初始值
        last_id = 0
    except ValueError:
        # 如果文件中的内容无法解析为整数，则返回0作为初始值
        last_id = 0

    return last_id


# 为指定表保存last_id（存储在last_ids文件夹中）
def save_last_id_for_table(table_name, last_id):
    # 确保last_ids文件夹存在
    last_ids_dir = "last_ids"
    if not os.path.exists(last_ids_dir):
        os.makedirs(last_ids_dir)
    
    table_id_file = os.path.join(last_ids_dir, f'{table_name}_last_id.txt')
    # 将最后一个处理的ID保存到表特定的文件中
    with open(table_id_file, mode='w') as file_object:
        file_object.write(str(last_id))

# 通过last_id获取Mysql更新数据
def get_new_inserted_data(last_id, cursor, table_name):
    try:
        # 在V2.2.0版本中，TDS的id从1开始自增
        query = f"SELECT * FROM {table_name} WHERE id > {last_id} limit {sync_max}"
        # 查询大于最后一次ID的新插入数据
        cursor.execute(query)
        new_data = cursor.fetchall()
        rownum_data = cursor.rowcount
        return rownum_data, new_data

    except (pymysql.err.OperationalError, pymysql.err.MySQLError) as e:
        print("MySQL连接错误:", e)
        return 0, []

