import iotdb
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import pymysql
from parameter_get import *
from id_get import *
from setting_log import *
import time
import datetime
from connection import *
from config import *

# Initialize the log
logger = setup_logging()

# Connect to IoTDB
logger.info("Connecting to IoTDB...")
session = get_session_connection()
logger.info(f"IoTDB connected, timezone: {session.get_time_zone()}")
# Connect to MySQL
logger.info("Connecting to MySQL...")
mysql_cursor, mysql_db = get_mysql_connection()
logger.info("MySQL connected")

############# Transfer_data
# Maintain independent last_id for each table
table_names = ["cryogenic_data", "quenchsc_data", "tds_data", "technique_data", "vacuum_data"]
table_last_ids = {}
for table_name in table_names:
    table_last_ids[table_name] = get_last_id_for_table(table_name)
# Loop to listen for data changes
if sync_count == 0:
    loop_count = None  # Infinite loop
else:
    loop_count = sync_count

current_count = 0
table_index = 0  # The index of the current table being processed

while loop_count is None or current_count < loop_count:
    try:
        # Get the current table name to be processed
        table_name = table_names[table_index]
        last_id = table_last_ids[table_name]
        
        logger.info(f"Processing table: {table_name}")
        
        # Get the new data and the number of rows according to last_id
        rownum_data, new_data = get_new_inserted_data(last_id, mysql_cursor, table_name) 

        # Get the parameters of IoTDB, and divide them according to device_size
        device_size = 60
        measurements_lsts, data_type_lsts = parameter_choice(table_name, mysql_cursor)
        measurements_lst = slicing_list(measurements_lsts, device_size)
        data_type_lst = slicing_list(data_type_lsts, device_size)
        devices = devices_get(measurements_lsts, device_size, table_name)

        ############# Prepare to insert data
        if new_data:
            logger.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Sync Start   id:{last_id}")
            start_time = time.perf_counter()

            # The real total number of inserted data
            real_datanum_all = 0

            # Loop through the updated data in Mysql
            for row in new_data:
                # If the time of this row is empty, skip this row
                if row[timerow_get(table_name)] is None or row[timerow_get(table_name)] == " ":
                    continue

                # Convert the timestamp to long, and convert the value to a tuple and slice it
                timestamp = convert_datetime_to_long(row[timerow_get(table_name)])
                values = tuple(row)
                value = slicing_list(values, device_size)

                # Loop to insert
                for i in range(len(devices)):
                    # List all parameters of this device
                    measurement = list(measurements_lst[i])
                    data_type = list(data_type_lst[i])
                    val = list(value[i])

                    # Replace the time field name with curr_time
                    measurement = ["curr_time" if m == "time" else m for m in measurement]
                    # Process the value and type
                    new_val = []
                    new_measurement = []
                    new_data_type = []
                    for v, m, d in zip(val, measurement, data_type):
                        if isinstance(v, datetime.datetime):
                            v = v.strftime("%Y-%m-%d %H:%M:%S")
                        if v is not None and v != "":
                            new_val.append(v)
                            new_measurement.append(m)
                            new_data_type.append(d)
                    val = new_val
                    measurement = new_measurement
                    data_type = new_data_type

                    # Count the number , inserted data
                    real_datanum_all += len(val)
                    session.insert_record(
                        f"root.{table_name}.{devices[i]}", timestamp, measurement, data_type, val
                    )

            taketime = time.perf_counter() - start_time
            last_id = max(row[0] for row in new_data)
            table_last_ids[table_name] = last_id
            # Completion of a insertion
            logger.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Sync end   id:{last_id} \nDuration: {taketime:.3f}s   Time Points: {real_datanum_all}\n ")
            save_last_id_for_table(table_name, last_id)

        # Mysql no new data
        else:
            logger.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} No New Data for {table_name}!\n")

        # next table ( if all table had been done, next loop )
        table_index = (table_index + 1) % len(table_names)
        if table_index == 0:
            current_count += 1
    
        time.sleep(sync_interval)

    #try except
    except (KeyboardInterrupt, SystemExit):
        logger.info("Program interrupted by user, exiting...")
        break
    except Exception as e:
        logger.error(f"Error occurred while processing {table_name}: {str(e)}")
        logger.info("Retrying in 5 seconds...")
        time.sleep(5)
        continue

############# connection disclose
logger.info("Closing database connections...")
close_mysql_connection(mysql_cursor, mysql_db)
session.close()
logger.info("All connections closed, program ended")

