from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    length,
    current_date
)
from datetime import datetime
import json


# ================= TERMINAL COLORS =================
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"


# ================= CONNECTION =================
with open("connection/conn.json") as conn:
    connection_parameters = json.load(conn)

session = Session.builder.configs(connection_parameters).create()


# ================= LOAD CONFIG TABLE =================
dq_config_df = (
    session.table("DEMO_DB.PUBLIC.DQ_CONFIG")
    .filter(col("IS_ACTIVE") == True)
)

# ================= LOAD RULE LOOKUP =================
dq_rules_rows = session.table("DEMO_DB.PUBLIC.DQ_RULES").collect()

rule_lookup = {
    r["RULE_ID"]: r["RULE_NAME"]
    for r in dq_rules_rows
}

print(f"\n{BOLD}================ DQ EXECUTION STARTED (CURRENT DAY) ================ {RESET}")


# ================= GROUP RULES BY TABLE =================
table_groups = {}

for row in dq_config_df.to_local_iterator():

    table_key = (
        row["DATABASE_NAME"],
        row["SCHEMA_NAME"],
        row["TABLE_NAME"]
    )

    table_groups.setdefault(table_key, []).append(row)


# ================= PROCESS EACH TABLE =================
for (database, schema_name, table), rules in table_groups.items():

    source_table = f"{database}.{schema_name}.{table}"

    # 🔥 IMPORTANT CHANGE → Filter for Current Day
    df = session.table(source_table).filter(
        col("LOAD_DATE") == current_date()
    )

    total_count = df.count()

    print(f"\n{BOLD}Table Checked : {table}{RESET}")
    print(f"{BOLD}Current Day Records : {total_count}{RESET}\n")

    # If no data for today, skip table
    if total_count == 0:
        print(f"{RED}No records found for current day. Skipping...{RESET}")
        continue

    for row in rules:

        start_time = datetime.now()

        rule_id = row["RULE_ID"]
        rule_type = rule_lookup.get(rule_id)

        if rule_type is None:
            print(f"Skipping unknown rule_id: {rule_id}")
            continue

        column_name = row["COLUMN_NAMES"]
        min_val = row["MIN_VALUE"]
        max_val = row["MAX_VALUE"]
        threshold = float(row["THRESHOLD"])
        severity = row["SEVERITY"]
        executed_by = row["CREATED_BY"]

        # ================= RULE LOGIC =================

        if rule_type == "NULL_CHECK":

            failed_df = df.filter(col(column_name).is_null())
            failed_count = failed_df.count()
            rule_expression = f"{column_name} IS NOT NULL"

        elif rule_type == "RANGE_CHECK":

            failed_df = df.filter(
                (col(column_name) < min_val) |
                (col(column_name) > max_val)
            )
            failed_count = failed_df.count()
            rule_expression = f"{column_name} BETWEEN {min_val} AND {max_val}"

        elif rule_type == "MIN_LENGTH_CHECK":

            failed_df = df.filter(
                col(column_name).is_not_null() &
                (length(col(column_name)) < min_val)
            )
            failed_count = failed_df.count()
            rule_expression = f"LENGTH({column_name}) >= {min_val}"

        elif rule_type == "DUPLICATE_CHECK":

            col_list = [c.strip() for c in column_name.split(",")]

            dup_df = (
                df.group_by([col(c) for c in col_list])
                  .count()
                  .filter(col("COUNT") > 1)
            )

            failed_df = dup_df
            failed_count = dup_df.count()
            rule_expression = f"DUPLICATE CHECK ON ({column_name})"

        else:
            print(f"Unsupported rule type: {rule_type}")
            continue


        # ================= METRICS =================
        passed_count = total_count - failed_count

        failure_percentage = (
            failed_count / total_count if total_count > 0 else 0
        )

        threshold_breached = failure_percentage > threshold
        rule_status = "FAIL" if threshold_breached else "PASS"

        failed_sample = failed_df.limit(5).collect()
        failed_sample_json = [r.as_dict() for r in failed_sample]

        color = RED if rule_status == "FAIL" else GREEN

        print("-------------------------------------------")
        print(f"Rule ID         : {rule_id}")
        print(f"Rule Type       : {rule_type}")
        print(f"Column(s)       : {column_name}")
        print(f"Failed Records  : {failed_count}")
        print(f"Failure %       : {round(failure_percentage,4)}")
        print(f"Threshold       : {threshold}")
        print(f"Status          : {color}{rule_status}{RESET}")
        print("-------------------------------------------\n")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        query_id = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
        warehouse = session.get_current_warehouse()

        # ================= INSERT RESULT =================
        result_row = {
            "RULE_ID": rule_id,
            "RULE_TYPE": rule_type,
            "DATABASE_NAME": database,
            "SCHEMA_NAME": schema_name,
            "TABLE_NAME": table,
            "COLUMN_NAME": column_name,
            "RULE_EXPRESSION": rule_expression,
            "THRESHOLD": threshold,
            "SEVERITY": severity,
            "TOTAL_RECORD_COUNT": total_count,
            "FAILED_RECORD_COUNT": failed_count,
            "PASSED_RECORD_COUNT": passed_count,
            "FAILURE_PERCENTAGE": failure_percentage,
            "RULE_STATUS": rule_status,
            "IS_THRESHOLD_BREACHED": threshold_breached,
            "START_TIME": start_time,
            "END_TIME": end_time,
            "EXECUTION_DURATION_SEC": duration,
            "QUERY_ID": query_id,
            "WAREHOUSE_NAME": warehouse,
            "SOURCE_TYPE": "TABLE",
            "SOURCE_LOCATION": source_table,
            "FAILED_SAMPLE_DATA": failed_sample_json,
            "ERROR_MESSAGE": None,
            "IS_ACTIVE": True,
            "EXECUTED_BY": executed_by,
            "EXECUTION_MODE": "SNOWPARK_INCREMENTAL",
            "CREATED_TIMESTAMP": datetime.now(),
            "UPDATED_TIMESTAMP": datetime.now()
        }

        session.create_dataframe([result_row]) \
               .write.mode("append") \
               .save_as_table("DEMO_DB.PUBLIC.DQ_RESULT_TABLE")


print(f"{BOLD}✅ DQ EXECUTION COMPLETED SUCCESSFULLY (CURRENT DAY DATA){RESET}\n")