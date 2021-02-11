import unittest
from unittest.mock import patch, call

from woodmac.sensor_data_processor import ProcessSensorData


class DataToQueuePipelineTest(unittest.TestCase):

    def setUp(self) -> None:
        self.TEST_DATA = [{'id': 9617, 'type': 'Sensor', 'content': {'temperature_f': 49.2, 'time_of_measurement':
            '2021-02-10T17:24:15.729381'}}, {'id': 9618, 'type': 'Sensor', 'content':
            {'temperature_f': 30.0, 'time_of_measurement': '2021-02-10T17:24:15.569281'}}]
        self.TRANSFORMED_DATA = [{'id': 9617, 'type': 'Sensor', 'content': {'temperature_c': 9.6, 'time_of_measurement':
            '2021-02-10T17:24:15.729381'}}, {'id': 9618, 'type': 'Sensor', 'content':
            {'temperature_c': -1.1, 'time_of_measurement': '2021-02-10T17:24:15.569281'}}]

    @patch('woodmac.sensor_data_processor.generate_sensor_data')
    @patch.object(ProcessSensorData, "_validate_data_for_transform")
    def test_data_to_queue_genrated_data(self, is_valid, genrated_data):
        genrated_data.return_value = self.TEST_DATA
        process_sensor_data = ProcessSensorData()
        is_valid.side_effect = [True, True]
        process_sensor_data.data_to_queue_pipeline()
        self.assertEqual(process_sensor_data._sensor_data_queue.popleft(), self.TRANSFORMED_DATA[0])
        self.assertEqual(process_sensor_data._sensor_data_queue.popleft(), self.TRANSFORMED_DATA[1])

    @patch('woodmac.sensor_data_processor.generate_sensor_data')
    @patch.object(ProcessSensorData, "_validate_data_for_transform")
    def test_data_to_queue_ingenrated_data(self, is_valid, genrated_data):
        genrated_data.return_value = self.TEST_DATA
        process_sensor_data = ProcessSensorData()
        is_valid.side_effect = [False, False]
        process_sensor_data.data_to_queue_pipeline()
        self.assertEqual(len(process_sensor_data._sensor_data_queue), 0)

    @patch('woodmac.sensor_data_processor.generate_sensor_data')
    @patch.object(ProcessSensorData, "_validate_data_for_transform")
    def test_data_to_queue_1valid_1invalid(self, is_valid, genrated_data):
        genrated_data.return_value = self.TEST_DATA
        process_sensor_data = ProcessSensorData()
        is_valid.side_effect = [False, True]
        process_sensor_data.data_to_queue_pipeline()
        self.assertEqual(len(process_sensor_data._sensor_data_queue), 1)
        self.assertEqual(process_sensor_data._sensor_data_queue.popleft(), self.TRANSFORMED_DATA[1])

    @patch('woodmac.sensor_data_processor.generate_sensor_data')
    @patch.object(ProcessSensorData, "_validate_data_for_transform")
    def test_data_to_queue_empty_data(self, is_valid, genrated_data):
        genrated_data.return_value = []
        process_sensor_data = ProcessSensorData()
        process_sensor_data.data_to_queue_pipeline()
        self.assertEqual(len(process_sensor_data._sensor_data_queue), 0)


class QueueToDatabasePipelineTest(unittest.TestCase):

    def setUp(self) -> None:
        self.TEST_DATA = [{'id': 9617, 'type': 'Sensor', 'content': {'temperature_f': 49.2, 'time_of_measurement':
            '2021-02-10T17:24:15.729381'}}, {'id': 9618, 'type': 'Sensor', 'content':
            {'temperature_f': 30.0, 'time_of_measurement': '2021-02-10T17:24:15.569281'}}]
        self.TRANSFORMED_DATA = [{'id': 9617, 'type': 'Sensor', 'content': {'temperature_c': 9.6, 'time_of_measurement':
            '2021-02-10T17:24:15.729381'}}, {'id': 9618, 'type': 'Sensor', 'content':
            {'temperature_c': -1.1, 'time_of_measurement': '2021-02-10T17:24:15.569281'}}]

    @patch('woodmac.sensor_data_processor.insert_sensor_data')
    @patch('woodmac.sensor_data_processor.generate_sensor_data')
    @patch.object(ProcessSensorData, "_validate_data_for_transform")
    def test_queue_to_database_positive(self, is_valid, generated_data, insert):
        # Populate Queue first
        generated_data.return_value = self.TEST_DATA
        process_sensor_data = ProcessSensorData()
        is_valid.side_effect = [True, True]
        process_sensor_data.data_to_queue_pipeline()
        process_sensor_data.queue_to_database_pipeline()
        insert.assert_has_calls([call(self.TRANSFORMED_DATA[0]["id"], self.TRANSFORMED_DATA[0]),
                                 call(self.TRANSFORMED_DATA[1]["id"], self.TRANSFORMED_DATA[1])])


class ValidateDataForTransformTest(unittest.TestCase):
    TEST_DATA_NO_ID = {'type': 'Sensor', 'content':
                       {'temperature_f': 30.0, 'time_of_measurement': '2021-02-10T17:24:15.569281'}}
    TEST_DATA_NO_DATA_TYPE = {'id': 9618, 'content':
        {'temperature_f': 30.0, 'time_of_measurement': '2021-02-10T17:24:15.569281'}}
    TEST_DATA_NO_TEMP_F = {'id': 9618, 'type': 'Sensor', 'content':
        {'time_of_measurement': '2021-02-10T17:24:15.569281'}}
    TEST_DATA_NO_TIME = {'id': 9618, 'type': 'Sensor', 'content':
        {'temperature_f': 30.0}}

    PROCESS_SENSOR_DATA = ProcessSensorData()

    def test_validate_no_id_key(self):
        self.assertEqual(self.PROCESS_SENSOR_DATA._validate_data_for_transform(self.TEST_DATA_NO_ID), False)

    def test_validate_no_data_type_key(self):
        self.assertEqual(self.PROCESS_SENSOR_DATA._validate_data_for_transform(self.TEST_DATA_NO_DATA_TYPE), False)

    def test_validate_no_temp_f_key(self):
        self.assertEqual(self.PROCESS_SENSOR_DATA._validate_data_for_transform(self.TEST_DATA_NO_TEMP_F), False)

    def test_validate_no_time_key(self):
        self.assertEqual(self.PROCESS_SENSOR_DATA._validate_data_for_transform(self.TEST_DATA_NO_TIME), False)


if __name__ == '__main__':
    unittest.main()


#  NOTE: Other modules would have been tested, but hasn't due to time constraints
