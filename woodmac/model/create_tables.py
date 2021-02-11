"""
Creates tables required for storing Sensor Data, noting happens if table is already created.
Postgres Database provider for this project is Elephantsql (https://www.elephantsql.com)
"""

from woodmac.model.database_driver import PostgresDBDriver


class GlobalIdentifierTable:

    _TABLE_NAME = "global_identifiers"
    _IDENTIFIER_FIELD = "global_id"

    def create(self) -> None:
        with PostgresDBDriver() as con:
            cur = con.cursor()
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._TABLE_NAME}
            (
             {self._IDENTIFIER_FIELD} serial PRIMARY KEY
            )
            """)
            con.commit()
        print(f"Table with name {self._TABLE_NAME} created!")

    @property
    def table_name(self):
        return self._TABLE_NAME

    @property
    def identifier_field_name(self):
        return self._IDENTIFIER_FIELD


class SensorDataTable:

    _TABLE_NAME = "sensor_data"
    _IDENTIFIER_FIELD = "global_id"
    _SENSOR_INFO_FIELD = "sensor_info"

    def create(self) -> None:
        with PostgresDBDriver() as con:
            cur = con.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._TABLE_NAME}
                (
                 {self._IDENTIFIER_FIELD} integer PRIMARY KEY REFERENCES {GlobalIdentifierTable().table_name},
                 {self._SENSOR_INFO_FIELD} json NOT NULL
                )
                """)
            con.commit()
        print(f"Table with name {self._TABLE_NAME} created!")

    @property
    def table_name(self):
        return self._TABLE_NAME

    @property
    def sensor_data_field_name(self):
        return self._SENSOR_INFO_FIELD

    @property
    def identifier_field_name(self):
        return self._IDENTIFIER_FIELD


if __name__ == "__main__":
    GlobalIdentifierTable().create()
    SensorDataTable().create()
