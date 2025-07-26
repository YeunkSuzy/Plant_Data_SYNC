import iotdb
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import datetime

############# IoTDB_parameter_get
def parameter_choice(table_name, cursor):
        measurements_lsts = [desc[0] for desc in cursor.description]
        if table_name == "cryogenic_data":
            data_type_lsts = [TSDataType.FLOAT] * 600
            data_type_lsts.insert(0,TSDataType.TEXT)
            data_type_lsts.insert(0, TSDataType.INT32)
            data_type_lsts.append(TSDataType.TEXT)
        elif table_name == "quenchsc_data":
            data_type_lsts = [TSDataType.FLOAT] * 127
            data_type_lsts.insert(0,TSDataType.TEXT)
            data_type_lsts.insert(0, TSDataType.INT32)
            data_type_lsts.append(TSDataType.TEXT)
        elif table_name == "tds_data":
            data_type_lsts_1 = [TSDataType.FLOAT] * 815
            data_type_lsts_1.insert(0, TSDataType.INT32)
            data_type_lsts_2 = [TSDataType.FLOAT] * 16
            data_type_lsts_2.insert(0,TSDataType.TEXT)
            data_type_lsts_2.insert(0, TSDataType.TEXT)
            data_type_lsts = data_type_lsts_1 + data_type_lsts_2
        elif table_name == "technique_data":
            data_type_lsts = [TSDataType.FLOAT] * 82
            data_type_lsts.insert(0, TSDataType.TEXT)
            data_type_lsts.insert(0, TSDataType.INT32)
        elif table_name == "vacuum_data":
            data_type_lsts_1 = [TSDataType.FLOAT] * 81
            data_type_lsts_1.insert(0, TSDataType.TEXT)
            data_type_lsts_1.insert(0, TSDataType.INT32)
            data_type_lsts_1.append(TSDataType.INT64)
            data_type_lsts_2 = [TSDataType.FLOAT] * 102
            data_type_lsts = data_type_lsts_1 + data_type_lsts_2
        return measurements_lsts, data_type_lsts
def slicing_list(lists, part_size):
    slice = []
    for i in range(0, len(lists), part_size):
        part_list = lists[i: i + part_size]
        slice.append(part_list)
    return (slice)
# get devices list
def devices_get(measurements_lsts, part_size, table_name):
        devices = []
        len_device = len(measurements_lsts) // part_size
        if len(measurements_lsts) % part_size != 0:
            len_device = len_device + 1
        for i in range(len_device):
            devices.append(f"dev{i + 1}")
        return (devices)
def timerow_get(table_name):
        if table_name == "tds_data":
            timerow = 816
        else:
            timerow = 1
        return timerow
# convert datetime to long
def convert_datetime_to_long(dt):
    dt_start = datetime.datetime(1970, 1, 1)
    to_now = dt - dt_start
    timestamp = int(to_now.total_seconds())
    return timestamp * 1000 - 28800000
