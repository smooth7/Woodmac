import json

from woodmac.model.database_driver import PostgresDBDriver
from woodmac.model.create_tables import GlobalIdentifierTable, SensorDataTable


def generate_new_global_id():
    with PostgresDBDriver() as con:
        cur = con.cursor()
        cur.execute(f"""
            INSERT INTO {GlobalIdentifierTable().table_name} values(default) 
            RETURNING {GlobalIdentifierTable().identifier_field_name};
        """)
        global_id = cur.fetchone()[0]
        con.commit()
    return global_id


def insert_sensor_data(global_id: int, sensor_info: dict):
    sen_table = SensorDataTable()
    with PostgresDBDriver() as con:
        cur = con.cursor()
        query_sql = (f"""
            INSERT INTO {sen_table.table_name} ({sen_table.identifier_field_name},
                     {sen_table.sensor_data_field_name}) values({global_id}, %s);
            """)
        cur.execute(query_sql, (json.dumps(sensor_info),))
        con.commit()
