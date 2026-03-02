
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import uuid
from datetime import datetime

def run_dq_for_inventory(session: Session):

    run_id = str(uuid.uuid4())

    configs = session.table("fact_OBJECT") \
        .filter(col("IS_ACTIVE") == True) \
        .filter(col("TABLE_NAME") == 'INVENTORY') \
        .collect()

    for config in configs:

        rule = session.table("Dim_RULE") \
            .filter(col("RULE_ID") == config["RULE_ID"]) \
            .collect()[0]

        table = f"{config['TABLE_CATALOG']}.{config['TABLE_SCHEMA']}.{config['TABLE_NAME']}"
        column = config["COLUMN_NAME"]

        # Build rule condition dynamically

        if rule["RULE_TYPE"] == "NOT_NULL":
            condition = f"{column} IS NOT NULL"

        elif rule["RULE_TYPE"] == "COMPARISON":
            condition = f"{column} {rule['OPERATOR']} {rule['VALUE_1']}"

        elif rule["RULE_TYPE"] == "RANGE":
            condition = f"{column} BETWEEN {rule['VALUE_1']} AND {rule['VALUE_2']}"

        elif rule["RULE_TYPE"] == "BINARY":
            condition = f"{column} {rule['OPERATOR']} {rule['VALUE_1']}"

        elif rule["RULE_TYPE"] == "DOMAIN":
            condition = f"{column} IN ({rule['VALUE_1']})"

        else:
            continue

        total_sql = f"SELECT COUNT(*) FROM {table}"
        failed_sql = f"""
            SELECT COUNT(*) 
            FROM {table}
            WHERE NOT ({condition})
        """

        total_count = session.sql(total_sql).collect()[0][0]
        failed_count = session.sql(failed_sql).collect()[0][0]

        failed_pct = (failed_count / total_count) * 100 if total_count > 0 else 0

        status = "PASS" if failed_pct <= config["THRESHOLD_PCT"] else "FAIL"

        insert_sql = f"""
        INSERT INTO RESULT_DETAIL
        VALUES (
            '{run_id}',
            '{config["TABLE_NAME"]}',
            '{column}',
            {config["RULE_ID"]},
            {total_count},
            {failed_count},
            {round(failed_pct,2)},
            {config["THRESHOLD_PCT"]},
            '{status}',
            CURRENT_TIMESTAMP()
        )
        """

        session.sql(insert_sql).collect()

    return f"DQ Run Completed. RUN_ID: {run_id}"
