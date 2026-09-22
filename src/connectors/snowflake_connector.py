from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
import json
import os


class SnowflakeConnector:

    def create_session(self):

        execution_mode = os.getenv(
            "DQM_CONNECTION_MODE",
            "LOCAL"
        ).upper()

        if execution_mode == "AIRFLOW":

            return self._create_airflow_session()

        return self._create_local_session()

    # =========================================================
    # LOCAL
    # =========================================================

    def _create_local_session(self):

        connection_name = os.getenv(
            "SNOWFLAKE_CONNECTION",
            "UDX_CORE_UAT"
        ).upper()

        connection_files = {
            "UDX_CORE_UAT": os.getenv(
                "UDX_CORE_UAT_CONNECTION_FILE",
                "/Users/206909593/DQ/connection/"
                "conn_udx_core_uat.json"
            ),
            "DQT": os.getenv(
                "DQT_CONNECTION_FILE",
                "/Users/206909593/DQ/connection/"
                "conn_dqt.json"
            )
        }

        if connection_name not in connection_files:
            raise ValueError(
                f"Invalid SNOWFLAKE_CONNECTION: "
                f"{connection_name}"
            )

        conn_file_path = connection_files[connection_name]

        if not os.path.exists(conn_file_path):
            raise FileNotFoundError(
                f"Snowflake connection file not found: "
                f"{conn_file_path}"
            )

        with open(conn_file_path, "r") as conn_file:
            connection_parameters = json.load(conn_file)

        private_key_content = connection_parameters.get(
            "private_key_content"
        )

        if not private_key_content:
            raise ValueError(
                "private_key_content is missing"
            )

        return self._create_session_from_key(
            connection_parameters,
            private_key_content,
            conn_file_path
        )

    # =========================================================
    # AIRFLOW / POLAR
    # =========================================================

    def _create_airflow_session(self):

        required_variables = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_PRIVATE_KEY",
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        ]

        missing = [
            variable
            for variable in required_variables
            if not os.getenv(variable)
        ]

        if missing:
            raise ValueError(
                "Missing Snowflake environment variables: "
                + ", ".join(missing)
            )

        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "insecure_mode": True
        }

        private_key_content = os.getenv(
            "SNOWFLAKE_PRIVATE_KEY"
        )

        return self._create_session_from_key(
            connection_parameters,
            private_key_content,
            "Airflow connection"
        )

    # =========================================================
    # COMMON PRIVATE KEY PROCESSING
    # =========================================================

    def _create_session_from_key(
        self,
        connection_parameters,
        private_key_content,
        source
    ):

        private_key_content = (
            private_key_content
            .replace("\\n", "\n")
            .strip()
        )

        private_key_passphrase = os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )

        if not private_key_passphrase:
            raise ValueError(
                "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE "
                "is not set"
            )

        try:

            p_key = serialization.load_pem_private_key(
                private_key_content.encode("utf-8"),
                password=private_key_passphrase.encode("utf-8")
            )

        except Exception as e:

            raise ValueError(
                f"Failed to load private key from {source}. "
                "Please verify PEM format and passphrase."
            ) from e

        try:

            private_key = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

        except Exception as e:

            raise ValueError(
                "Failed to convert private key "
                "to DER PKCS8 format."
            ) from e

        connection_parameters["private_key"] = private_key

        try:

            session = (
                Session.builder
                .configs(connection_parameters)
                .create()
            )

        except Exception as e:

            raise RuntimeError(
                f"Failed to create Snowflake session "
                f"using {source}: {e}"
            ) from e

        return session