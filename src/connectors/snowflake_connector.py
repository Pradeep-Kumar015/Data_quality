from snowflake.snowpark import Session
import json


class SnowflakeConnector:

    def create_session(self):

        with open("connection/conn.json") as conn:
            connection_parameters = json.load(conn)

        session = Session.builder.configs(connection_parameters).create()

        return session