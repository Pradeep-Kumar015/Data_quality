from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
import json
import os


class SnowflakeConnector:

    def create_session(self):

        # ---------------------------------------------------------
        # 1. Select Snowflake connection
        # ---------------------------------------------------------
        connection_name = os.getenv(
            "SNOWFLAKE_CONNECTION",
            "UDX_CORE_UAT"
        ).upper()

        connection_files = {
            "UDX_CORE_UAT": (
                "/Users/206909593/DQ/connection/"
                "conn_udx_core_uat.json"
            ),
            "DQT": (
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

        # ---------------------------------------------------------
        # 2. Validate connection file
        # ---------------------------------------------------------
        if not os.path.exists(conn_file_path):
            raise FileNotFoundError(
                f"Snowflake connection file not found: "
                f"{conn_file_path}"
            )

        # ---------------------------------------------------------
        # 3. Read connection JSON
        # ---------------------------------------------------------
        with open(conn_file_path, "r") as conn_file:
            connection_parameters = json.load(conn_file)

        # ---------------------------------------------------------
        # 4. Get private key content from JSON
        # ---------------------------------------------------------
        private_key_content = connection_parameters.get(
            "private_key_content"
        )

        if not private_key_content:
            raise ValueError(
                f"private_key_content is missing in "
                f"{conn_file_path}"
            )

        # ---------------------------------------------------------
        # 5. Normalize private key content
        #
        # JSON may contain literal escaped newline characters:
        #
        # -----BEGIN ENCRYPTED PRIVATE KEY-----\nABC...\n-----
        #
        # Convert them into actual newline characters.
        # ---------------------------------------------------------
        private_key_content = private_key_content.replace(
            "\\n",
            "\n"
        )

        # Remove accidental leading/trailing whitespace
        private_key_content = private_key_content.strip()

        # ---------------------------------------------------------
        # 6. Get private key passphrase
        # ---------------------------------------------------------
        private_key_passphrase = os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )

        if not private_key_passphrase:
            raise ValueError(
                "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE "
                "environment variable is not set"
            )

        # ---------------------------------------------------------
        # 7. Load encrypted private key
        # ---------------------------------------------------------
        try:

            p_key = serialization.load_pem_private_key(
                private_key_content.encode("utf-8"),
                password=private_key_passphrase.encode("utf-8")
            )

        except Exception as e:

            raise ValueError(
                "Failed to load private key from "
                f"'private_key_content' in {conn_file_path}. "
                "Please verify the PEM format and passphrase."
            ) from e

        # ---------------------------------------------------------
        # 8. Convert private key to DER PKCS8
        # ---------------------------------------------------------
        try:

            private_key = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

        except Exception as e:

            raise ValueError(
                "Failed to convert private key to "
                "DER PKCS8 format."
            ) from e

        # ---------------------------------------------------------
        # 9. Remove private key content from Snowflake parameters
        #
        # Snowflake should receive the actual private key bytes,
        # not the encrypted PEM string.
        # ---------------------------------------------------------
        connection_parameters.pop(
            "private_key_content",
            None
        )

        connection_parameters["private_key"] = private_key

        # ---------------------------------------------------------
        # 10. Create Snowflake session
        # ---------------------------------------------------------
        try:

            session = (
                Session.builder
                .configs(connection_parameters)
                .create()
            )

        except Exception as e:

            raise RuntimeError(
                f"Failed to create Snowflake session "
                f"for connection '{connection_name}' "
                f"using {conn_file_path}: {e}"
            ) from e

        return session

