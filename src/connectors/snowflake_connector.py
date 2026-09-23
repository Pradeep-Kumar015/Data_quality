from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization

import os
import logging


logger = logging.getLogger(__name__)


class SnowflakeConnector:

    AIRFLOW_CONNECTION_ID = "snowflake_maximo_conn"

    def create_session(self):
        """
        Create a Snowpark Snowflake session using the
        existing Airflow connection.

        Airflow Connection:
            Login    -> Snowflake user
            Password -> Private-key passphrase
            Extra    -> Snowflake connection details and
                       encrypted private-key content
        """

        # =====================================================
        # GET AIRFLOW BASEHOOK
        # =====================================================

        try:
            from airflow.hooks.base import BaseHook
        except Exception as e:
            raise RuntimeError(
                "Unable to import Airflow BaseHook. "
                "This DQM framework must run inside Airflow."
            ) from e

        # =====================================================
        # GET AIRFLOW CONNECTION ID
        # =====================================================

        connection_id = os.getenv(
            "SNOWFLAKE_AIRFLOW_CONNECTION_ID",
            self.AIRFLOW_CONNECTION_ID
        )

        logger.info(
            "Using Airflow Snowflake connection '%s'",
            connection_id
        )

        # =====================================================
        # GET AIRFLOW CONNECTION
        # =====================================================

        try:
            airflow_connection = (
                BaseHook.get_connection(connection_id)
            )
        except Exception as e:
            raise RuntimeError(
                "Unable to retrieve Airflow connection "
                f"'{connection_id}': {e}"
            ) from e

        logger.info(
            "Airflow Snowflake connection "
            "'%s' retrieved successfully",
            connection_id
        )

        # =====================================================
        # GET EXTRA CONFIGURATION
        # =====================================================

        extra = airflow_connection.extra_dejson

        if not extra:
            raise ValueError(
                f"Airflow connection '{connection_id}' "
                "does not contain Extra configuration."
            )

        # =====================================================
        # VALIDATE REQUIRED EXTRA FIELDS
        # =====================================================

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

        # =====================================================
        # VALIDATE LOGIN
        # =====================================================

        if not airflow_connection.login:
            raise ValueError(
                f"Login/user is not configured in "
                f"Airflow connection '{connection_id}'."
            )

        # =====================================================
        # PRIVATE KEY PASSPHRASE
        #
        # Airflow Connection Password field contains
        # the encrypted Snowflake private-key passphrase.
        # =====================================================

        private_key_passphrase = airflow_connection.password

        if not private_key_passphrase:
            raise ValueError(
                f"Password field is empty in Airflow "
                f"connection '{connection_id}'. "
                "Please configure the Snowflake "
                "private-key passphrase in the "
                "Airflow Connection Password field."
            )

        logger.info(
            "Private-key passphrase retrieved from "
            "Airflow connection Password field"
        )

        # =====================================================
        # BUILD SNOWFLAKE CONNECTION PARAMETERS
        # =====================================================

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

        # =====================================================
        # OPTIONAL SCHEMA
        # =====================================================

        if airflow_connection.schema:
            connection_parameters["schema"] = (
                airflow_connection.schema
            )
        elif extra.get("schema"):
            connection_parameters["schema"] = (
                extra["schema"]
            )

        # =====================================================
        # PRIVATE KEY CONTENT
        # =====================================================

        private_key_content = extra.get(
            "private_key_content"
        )

        if not private_key_content:
            raise ValueError(
                f"private_key_content is missing from "
                f"Airflow connection '{connection_id}'."
            )

        private_key_content = (
            private_key_content
            .replace("\\n", "\n")
            .strip()
        )

        logger.info(
            "Private-key content retrieved from "
            "Airflow connection"
        )

        # =====================================================
        # LOAD ENCRYPTED PRIVATE KEY
        # =====================================================

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

            logger.info(
                "Snowflake private key loaded successfully"
            )

        except Exception as e:
            raise ValueError(
                "Failed to load private key from "
                f"Airflow connection '{connection_id}'. "
                "Please verify the PEM format and "
                "private-key passphrase in the "
                "Password field."
            ) from e

        # =====================================================
        # CONVERT PRIVATE KEY TO DER PKCS8
        # =====================================================

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

            logger.info(
                "Private key converted to DER PKCS8 format"
            )

        except Exception as e:
            raise ValueError(
                "Failed to convert private key "
                "to DER PKCS8 format."
            ) from e

        # =====================================================
        # ADD PRIVATE KEY TO CONNECTION PARAMETERS
        # =====================================================

        connection_parameters["private_key"] = (
            private_key
        )

        # =====================================================
        # CREATE SNOWFLAKE SESSION
        # =====================================================

        try:
            logger.info(
                "Creating Snowflake session using "
                "Airflow connection '%s'",
                connection_id
            )

            session = (
                Session.builder
                .configs(connection_parameters)
                .create()
            )

        except Exception as e:
            raise RuntimeError(
                "Failed to create Snowflake session "
                f"using Airflow connection "
                f"'{connection_id}': {e}"
            ) from e

        # =====================================================
        # SUCCESS
        # =====================================================

        logger.info(
            "Snowflake session created successfully "
            "using Airflow connection '%s'",
            connection_id
        )

        return session