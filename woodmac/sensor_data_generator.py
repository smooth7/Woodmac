import random
import datetime

from woodmac.model.database_interface import generate_new_global_id

DATA_TYPE_VALUE = "Sensor"

# Keys used in sensor data
ID_KEY = "id"
DATA_TYPE_KEY = "type"
CONTENT_KEY = "content"
TEMPERATURE_F_KEY = "temperature_f"
TEMPERATURE_C_KEY = "temperature_c"
TIME_OF_MEASUREMENT_KEY = "time_of_measurement"

DATA_COUNT_LOW = 1
DATA_COUNT_HIGH = 700
TEMP_F_LOW = 20
TEMP_F_HIGH = 90


def generate_sensor_data(data_count_low: int = DATA_COUNT_LOW, data_count_high: int = DATA_COUNT_HIGH,
                         temp_f_low: int = TEMP_F_LOW, temp_f_high: int = TEMP_F_HIGH):

    data_count = random.randint(data_count_low, data_count_high)
    for i in range(data_count):
        global_id = generate_new_global_id()
        temp_f = round(random.uniform(temp_f_low, temp_f_high), 1)
        date_time_iso_format = datetime.datetime.today().isoformat()
        sensor_data = {
            ID_KEY: global_id,
            DATA_TYPE_KEY: DATA_TYPE_VALUE,
            CONTENT_KEY: {
                TEMPERATURE_F_KEY: temp_f,
                TIME_OF_MEASUREMENT_KEY: date_time_iso_format
            }
        }
        yield sensor_data


