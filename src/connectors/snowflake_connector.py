from snowflake.snowpark import Session
import json
import os


class SnowflakeConnector:

    def create_session(self):

        # --------------------------------------------------
        # Resolve connection file path dynamically
        # --------------------------------------------------
        conn_file_path = os.getenv(
            "SNOWFLAKE_CONN_FILE",
            "/Users/206909593/DQ/connection/conn.json"
        )

        # --------------------------------------------------
        # Validate file existence
        # --------------------------------------------------
        if not os.path.exists(conn_file_path):
            raise FileNotFoundError(
                f"Snowflake connection file not found: {conn_file_path}"
            )

        # --------------------------------------------------
        # Load Snowflake credentials
        # --------------------------------------------------
        with open(conn_file_path, "r") as conn_file:

            connection_parameters = json.load(conn_file)

        # --------------------------------------------------
        # Create Snowpark session
        # --------------------------------------------------
        session = Session.builder.configs(
            connection_parameters
        ).create()

        return session