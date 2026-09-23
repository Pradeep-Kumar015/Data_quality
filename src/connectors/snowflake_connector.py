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
    # LOCAL MODE
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
                f"{connection_name}. "
                f"Expected one of: "
                f"{', '.join(connection_files.keys())}"
            )

        conn_file_path = connection_files[connection_name]

        if not os.path.exists(conn_file_path):
            raise FileNotFoundError(
                "Snowflake connection file not found: "
                f"{conn_file_path}"
            )

        with open(
            conn_file_path,
            "r",
            encoding="utf-8"
        ) as conn_file:

            connection_parameters = json.load(
                conn_file
            )

        private_key_content = (
            connection_parameters.get(
                "private_key_content"
            )
        )

        if not private_key_content:
            raise ValueError(
                "private_key_content is missing from "
                f"{conn_file_path}"
            )

        return self._create_session_from_key(
            connection_parameters=connection_parameters,
            private_key_content=private_key_content,
            source=conn_file_path
        )

    # =========================================================
    # AIRFLOW MODE
    # =========================================================

    def _create_airflow_session(self):

        try:

            from airflow.hooks.base import BaseHook

        except ImportError as e:

            raise RuntimeError(
                "Airflow is not available. "
                "DQM_CONNECTION_MODE=AIRFLOW requires "
                "the Airflow runtime."
            ) from e

        # -----------------------------------------------------
        # Determine which logical Snowflake connection is needed
        #
        # UDX_CORE_UAT -> source database
        # DQT          -> BI_DATA_QUALITY_UAT
        # -----------------------------------------------------

        connection_name = os.getenv(
            "SNOWFLAKE_CONNECTION",
            "UDX_CORE_UAT"
        ).upper()

        airflow_connection_ids = {
            "UDX_CORE_UAT": os.getenv(
                "SNOWFLAKE_AIRFLOW_CONNECTION_UDX_CORE_UAT"
            ),
            "DQT": os.getenv(
                "SNOWFLAKE_AIRFLOW_CONNECTION_DQT"
            )
        }

        if connection_name not in airflow_connection_ids:

            raise ValueError(
                f"Invalid SNOWFLAKE_CONNECTION: "
                f"{connection_name}. "
                f"Expected one of: "
                f"{', '.join(airflow_connection_ids.keys())}"
            )

        connection_id = airflow_connection_ids[
            connection_name
        ]

        if not connection_id:

            raise ValueError(
                f"Airflow connection ID is not configured "
                f"for SNOWFLAKE_CONNECTION={connection_name}. "
                f"Expected environment variable: "
                f"SNOWFLAKE_AIRFLOW_CONNECTION_"
                f"{connection_name}"
            )

        # -----------------------------------------------------
        # Retrieve existing Airflow Connection
        # -----------------------------------------------------

        try:

            airflow_connection = (
                BaseHook.get_connection(
                    connection_id
                )
            )

        except Exception as e:

            raise RuntimeError(
                f"Unable to retrieve Airflow connection "
                f"'{connection_id}' for "
                f"{connection_name}: {e}"
            ) from e

        # -----------------------------------------------------
        # Read Extra JSON
        # -----------------------------------------------------

        extra = airflow_connection.extra_dejson

        if not extra:

            raise ValueError(
                f"Airflow connection '{connection_id}' "
                "does not contain Extra configuration."
            )

        # -----------------------------------------------------
        # Validate required fields
        # -----------------------------------------------------

        required_extra_fields = [
            "account",
            "warehouse",
            "database",
            "role",
            "private_key_content"
        ]

        missing_fields = [
            field
            for field in required_extra_fields
            if not extra.get(field)
        ]

        if missing_fields:

            raise ValueError(
                f"Missing required fields in Airflow "
                f"connection '{connection_id}' Extra: "
                + ", ".join(missing_fields)
            )

        # -----------------------------------------------------
        # User comes from Airflow Connection Login
        # -----------------------------------------------------

        if not airflow_connection.login:

            raise ValueError(
                f"Login/user is not configured in "
                f"Airflow connection '{connection_id}'."
            )

        # -----------------------------------------------------
        # Build Snowflake connection parameters
        # -----------------------------------------------------

        connection_parameters = {
            "account": extra["account"],
            "user": airflow_connection.login,
            "warehouse": extra["warehouse"],
            "database": extra["database"],
            "role": extra["role"],
            "insecure_mode": extra.get(
                "insecure_mode",
                True
            )
        }

        # -----------------------------------------------------
        # Schema
        #
        # Prefer Airflow Connection Schema.
        # Otherwise use Extra schema if available.
        # -----------------------------------------------------

        if airflow_connection.schema:

            connection_parameters["schema"] = (
                airflow_connection.schema
            )

        elif extra.get("schema"):

            connection_parameters["schema"] = (
                extra["schema"]
            )

        # -----------------------------------------------------
        # Private key
        # -----------------------------------------------------

        private_key_content = (
            extra.get("private_key_content")
        )

        if not private_key_content:

            raise ValueError(
                f"private_key_content is missing from "
                f"Airflow connection '{connection_id}'."
            )

        # -----------------------------------------------------
        # Private key passphrase
        #
        # Passphrase is NOT stored in the connection JSON/Extra.
        # It must be available as an environment variable.
        # -----------------------------------------------------

        private_key_passphrase = os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )

        if not private_key_passphrase:

            raise ValueError(
                "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE "
                "is not set in Airflow."
            )

        # -----------------------------------------------------
        # Create Snowflake session
        # -----------------------------------------------------

        return self._create_session_from_key(
            connection_parameters=connection_parameters,
            private_key_content=private_key_content,
            source=(
                f"Airflow connection: "
                f"{connection_id} "
                f"({connection_name})"
            )
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

        if not private_key_content:

            raise ValueError(
                f"Private key content is empty. "
                f"Source: {source}"
            )

        # -----------------------------------------------------
        # Normalize escaped newline characters
        # -----------------------------------------------------

        private_key_content = (
            private_key_content
            .replace("\\n", "\n")
            .strip()
        )

        # -----------------------------------------------------
        # Get encrypted private-key passphrase
        # -----------------------------------------------------

        private_key_passphrase = os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )

        if not private_key_passphrase:

            raise ValueError(
                "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE "
                "is not set."
            )

        # -----------------------------------------------------
        # Load encrypted PEM private key
        # -----------------------------------------------------

        try:

            p_key = (
                serialization.load_pem_private_key(
                    private_key_content.encode("utf-8"),
                    password=(
                        private_key_passphrase
                        .encode("utf-8")
                    )
                )
            )

        except Exception as e:

            raise ValueError(
                f"Failed to load private key from "
                f"{source}. "
                "Please verify the PEM format and "
                "private-key passphrase."
            ) from e

        # -----------------------------------------------------
        # Convert private key to DER PKCS8
        # -----------------------------------------------------

        try:

            private_key = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=(
                    serialization.PrivateFormat.PKCS8
                ),
                encryption_algorithm=(
                    serialization.NoEncryption()
                )
            )

        except Exception as e:

            raise ValueError(
                "Failed to convert private key "
                "to DER PKCS8 format."
            ) from e

        # -----------------------------------------------------
        # Add private key to Snowflake configuration
        # -----------------------------------------------------

        connection_parameters["private_key"] = (
            private_key
        )

        # -----------------------------------------------------
        # Create Snowpark session
        # -----------------------------------------------------

        try:

            session = (
                Session.builder
                .configs(connection_parameters)
                .create()
            )

        except Exception as e:

            raise RuntimeError(
                "Failed to create Snowflake session "
                f"using {source}: {e}"
            ) from e

        return session