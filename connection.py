from iotdb.dbapi import connect
import pymysql
from config import *
from iotdb.Session import Session

# connect to IoTDB
def get_iotdb_connection():
    iotdb_db = connect(iotdb_ip, '6667', iotdb_user, iotdb_password, fetch_size=1024, zone_id="UTC+8", sqlalchemy_mode=False)
    iotdb_cursor = iotdb_db.cursor()
    return iotdb_cursor, iotdb_db

# connect to MySQL
def get_mysql_connection():
    mysql_db = pymysql.connect(host=mysql_ip, port=3306, user=mysql_user, password=mysql_password, database=mysql_database)
    mysql_cursor = mysql_db.cursor()      
    return mysql_cursor, mysql_db

# get IoTDB Session
def get_session_connection():
    session = Session(iotdb_ip, '6667', iotdb_user, iotdb_password)
    session.open(False)
    return session

# close database connections
def close_iotdb_connection(iotdb_cursor, iotdb_db):
    iotdb_cursor.close()
    iotdb_db.close()

def close_mysql_connection(mysql_cursor, mysql_db):
    mysql_cursor.close()
    mysql_db.close()

def close_all_connections(iotdb_cursor, iotdb_db, mysql_cursor, mysql_db):
    close_iotdb_connection(iotdb_cursor, iotdb_db)
    close_mysql_connection(mysql_cursor, mysql_db)
