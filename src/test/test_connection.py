import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from src.connectors.snowflake_connector import SnowflakeConnector


def main():
    try:
        session = SnowflakeConnector().create_session()

        print("✅ Snowflake Connection Successful")

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
        print(f"User       : {row[0]}")
        print(f"Role       : {row[1]}")
        print(f"Warehouse  : {row[2]}")
        print(f"Database   : {row[3]}")
        print(f"Schema     : {row[4]}")

        session.close()

        print("\n✅ Session Closed Successfully")

    except Exception as ex:
        print("\n❌ Connection Failed")
        print(str(ex))


if __name__ == "__main__":
    main()