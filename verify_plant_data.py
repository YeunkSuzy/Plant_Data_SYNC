import time
import math
import logging
from setting_log import *
from config import *
from connection import *

# check if two values are equal, support None, nan and float error tolerance
def float_equal(a, b, tol=1e-3):
    # check if two values are equal, support None, nan and float error tolerance
    if (a is None or (isinstance(a, float) and math.isnan(a))) and (b is None or (isinstance(b, float) and math.isnan(b))):
        return True
    try:
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=tol)
    except:
        return str(a) == str(b)

# verify plant data
def verify_data(batch_start, batch_end, logger, mysql_cursor, iotdb_cursor):
    # get mysql data
    mysql_cursor.execute(f"SELECT * FROM {table_name} WHERE id >= {batch_start} AND id <= {batch_end + 1}")
    mysql_rows = mysql_cursor.fetchall()
    mysql_field_names = [desc[0] for desc in mysql_cursor.description]
    # build all needed mapping to deal with the problem that multiple ids correspond to a time
    mysql_dict_all = {}
    id_to_time = {}
    time_to_ids = {}
    for row in mysql_rows:
        row_dict = dict(zip(mysql_field_names, row))
        i = row_dict['id']
        t = None
        for f in mysql_field_names:
            if f.lower() in ('time', 'datetime', 'curr_time'):
                t = row_dict[f]
                break
        mysql_dict_all[i] = row_dict
        id_to_time[i] = t
        if t not in time_to_ids:
            time_to_ids[t] = []
        time_to_ids[t].append(i)
    # get iotdb data
    iotdb_cursor.execute(f"SELECT * FROM root.{table_name}.* WHERE id >= {batch_start} AND id <= {batch_end + 1}")
    iotdb_rows = iotdb_cursor.fetchall()
    iotdb_field_names = [desc[0] for desc in iotdb_cursor.description]
    iotdb_dict_all = {dict(zip([name.split('.')[-1] for name in iotdb_field_names], row))['id']: dict(zip([name.split('.')[-1] for name in iotdb_field_names], row)) for row in iotdb_rows}
    # verify data
    for id in range(batch_start, batch_end + 1):
        mysql_dict = mysql_dict_all.get(id)
        iotdb_dict = iotdb_dict_all.get(id)
        if mysql_dict and iotdb_dict:
            is_consistent = True
            for field in mysql_dict:
                if field in iotdb_dict:
                    a, b = mysql_dict[field], iotdb_dict[field]
                    if not float_equal(a, b):
                        logger.info(f"id={id} field {field} mismatch: MySQL={a}, IoTDB={b}")
                        is_consistent = False
                else:
                    if field not in {"datetime", "time"}:
                        logger.info(f"id={id} field {field} missing in IoTDB")
                        is_consistent = False
            if is_consistent:
                logger.info(f"id={id} Data consistent")
        else:
            t = id_to_time.get(id)
            ids_with_same_time = time_to_ids.get(t, [])
            larger_ids = [i for i in ids_with_same_time if i > id]
            if larger_ids:
                logger.info(f"id={id} missing in IoTDB (possibly overwritten by id={min(larger_ids)} with same time)")
            else:
                logger.info(f"id={id} missing data on one side")

def main():
    # setup logging    
    logger, log_path = setup_verify_logging(table_name, id_start, id_end)
    # verify data
    start_time = time.perf_counter()
    try:
        for batch_start in range(id_start, id_end + 1, verify_batchsize):
            batch_end = min(batch_start + verify_batchsize - 1, id_end)
            # get database connections
            iotdb_cursor, iotdb_db = get_iotdb_connection()
            mysql_cursor, mysql_db = get_mysql_connection()
            verify_data(batch_start, batch_end, logger, mysql_cursor, iotdb_cursor)
        # close connections after each batch
        close_all_connections(iotdb_cursor, iotdb_db, mysql_cursor, mysql_db)
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
    finally:
        taketime = time.perf_counter() - start_time
        msg_time = f"Total time: {taketime:.3f} seconds"
        if 'logger' in locals():
            logger.info(msg_time)
        else:
            print(msg_time)

if __name__ == '__main__':
    main()
