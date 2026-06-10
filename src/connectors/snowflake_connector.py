from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
import json
import os


class SnowflakeConnector:

    def create_session(self):

        conn_file_path = os.getenv(
            "SNOWFLAKE_CONN_FILE",
            "/Users/206909593/DQ/connection/conn.json"
        )

        if not os.path.exists(conn_file_path):
            raise FileNotFoundError(
                f"Snowflake connection file not found: {conn_file_path}"
            )

        with open(conn_file_path, "r") as conn_file:
            connection_parameters = json.load(conn_file)

        # Load private key
        with open(
            connection_parameters["private_key_file"],
            "rb"
        ) as key_file:

            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )

        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Remove file path and replace with actual key
        connection_parameters.pop("private_key_file")

        connection_parameters["private_key"] = private_key

        session = Session.builder.configs(
            connection_parameters
        ).create()

        return session