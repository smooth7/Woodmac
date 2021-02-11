"""
This module implements a pipeline which takes generated data, processes them, and sends them through relevant pipelines
"""

import collections
import threading
import time
import numbers

from woodmac.sensor_data_generator import generate_sensor_data, ID_KEY, CONTENT_KEY, TEMPERATURE_F_KEY, \
    TEMPERATURE_C_KEY, DATA_TYPE_KEY, DATA_TYPE_VALUE, TIME_OF_MEASUREMENT_KEY
from woodmac.model.database_interface import insert_sensor_data


class ProcessSensorData:

    def __init__(self):
        self._sensor_data = generate_sensor_data()
        self._sensor_data_queue = collections.deque()

    def data_to_queue_pipeline(self) -> None:
        """Process all generated data and add to queue"""
        for data in self._sensor_data:
            is_data_valid = self._validate_data_for_transform(data)
            if not is_data_valid:
                continue
            print(data[ID_KEY])
            print(f"Transforming data with global id {data[ID_KEY]}, and writing to queue.")
            temp_celsius = round((data[CONTENT_KEY][TEMPERATURE_F_KEY] - 32) / 1.8, 1)
            del(data[CONTENT_KEY][TEMPERATURE_F_KEY])
            data[CONTENT_KEY][TEMPERATURE_C_KEY] = temp_celsius
            self._sensor_data_queue.append(data)

    def queue_to_database_pipeline(self) -> None:
        """Write data in queue to database"""
        if self._sensor_data_queue:
            data_to_save = self._sensor_data_queue.popleft()
            try:
                global_id = data_to_save[ID_KEY]
            except KeyError:
                # Highly unlikely to happen as the data was validated before writing to queue
                print(f"ERROR: Unexpected! The global ID key is not present and data would not be written to database")
                self.queue_to_database_pipeline()
            print(f"Saving data from queue with global id {global_id} to database")
            insert_sensor_data(global_id, data_to_save)
            # Recursive call to continue processing data in queue
            self.queue_to_database_pipeline()

    @staticmethod
    def _validate_data_for_transform(data: dict) -> bool:
        """Validate generated data before processing"""
        try:
            global_id = data[ID_KEY]
            _ = data[DATA_TYPE_KEY]
            temp_f = data[CONTENT_KEY][TEMPERATURE_F_KEY]
            _ = data[CONTENT_KEY][TIME_OF_MEASUREMENT_KEY]
        except KeyError:
            print(f"ERROR: The data {data} will not be processed because an expected key is missing")
            return False
        if not (isinstance(temp_f, numbers.Number)):
            print(f"ERROR: The data with global ID {global_id} will not be processed because {TEMPERATURE_F_KEY} "
                  f"the value provided is not a number")
            return False
        if data[DATA_TYPE_KEY] != DATA_TYPE_VALUE:
            print(f"ERROR: The data with global ID {global_id} will not be processed because the type key "
                  f"{DATA_TYPE_KEY} does not have the expected value")
            return False
        return True


def main() -> None:
    """Main method to execute pipeline. Uses concurrency."""
    sensor_obj = ProcessSensorData()
    data_to_queue_thread = threading.Thread(target=sensor_obj.data_to_queue_pipeline)
    print("\nStarting thread to process all available sensor data, and then adding them to queue")
    data_to_queue_thread.start()
    while data_to_queue_thread.is_alive():
        secs_to_wait = 0.5
        print(f"Waiting for {secs_to_wait} seconds before checking if any data is added to queue\n")
        time.sleep(secs_to_wait)
        print("\nProcessing data that may have been added to queue")
        sensor_obj.queue_to_database_pipeline()
    # After data_to_queue_thread ends, check the queue one more time to ensure all data is processed
    sensor_obj.queue_to_database_pipeline()


if __name__ == '__main__':
    main()
