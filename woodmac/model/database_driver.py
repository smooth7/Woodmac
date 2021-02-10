import psycopg2


class PostgresDBDriver:

    def __init__(self):
        self.connection = None

    # Note: In production, connection parameters (especially password) would be stored more securely
    # Also the password would be encrypted in production
    DB_NAME = "vbcychxd"
    DB_USER = "vbcychxd"
    DB_PASSWORD = "1zCZTW77XdlHu3xRY_x-sSIdLqSdT2md"
    DB_HOST = "kandula.db.elephantsql.com"
    DB_PORT = "5432"

    def __enter__(self):
        self.connection = psycopg2.connect(database=self.DB_NAME, user=self.DB_USER, password=self.DB_PASSWORD,
                                           host=self.DB_HOST, port=self.DB_PORT)
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.connection:
            self.connection.close()
