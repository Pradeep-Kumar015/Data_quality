import os
from dotenv import load_dotenv
from snowflake.snowpark import Session


class SnowflakeConnector:

    def create_session(self):
        """
        Create a Snowflake session using SSO (External Browser Authentication)
        """

        try:
            load_dotenv(override=True)

            connection_parameters = {
                "account": os.environ["SNOWFLAKE_ACCOUNT"],
                "user": os.environ["SNOWFLAKE_USER"],
                "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
                "database": os.environ["SNOWFLAKE_DATABASE"],
                "schema": os.environ["SNOWFLAKE_SCHEMA"],
                "role": os.environ["SNOWFLAKE_ROLE"],
                "authenticator": "externalbrowser"
            }
            
            print("connection_parameters", connection_parameters)

            session = Session.builder.configs(
                connection_parameters
            ).create()

            result = session.sql("""
                SELECT
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA()
            """).collect()

            row = result[0]

            print("\n===== Snowflake Session Details =====")
            print(f"User       : {row['CURRENT_USER()']}")
            print(f"Role       : {row['CURRENT_ROLE()']}")
            print(f"Warehouse  : {row['CURRENT_WAREHOUSE()']}")
            print(f"Database   : {row['CURRENT_DATABASE()']}")
            print(f"Schema     : {row['CURRENT_SCHEMA()']}")
            print("=====================================\n")

            return session

        except Exception as e:
            raise Exception(f"Failed to create Snowflake session: {str(e)}")