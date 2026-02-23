from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, length
from datetime import datetime
import json


# 🎨 Terminal Colors
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"


# <<<<<<<<<<<<<<<<<<<<<< CONNECTION >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

with open("connection/conn.json") as conn:
    connection_parameters = json.load(conn)

session = Session.builder.configs(connection_parameters).create()


# <<<<<<<<<<<<<<<<<<<<<< LOAD CONFIG FILES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

with open("config/config.json") as c:
    config = json.load(c)

with open("config/rules.json") as r:
    rules = json.load(r)

rule_lookup = {r["RULE_ID"]: r["RULE_TYPE"] for r in rules}


# <<<<<<<<<<<<<<<<<<<<<< BASIC VARIABLES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

database = config["database"]
schema_name = config["schema"]
table = config["table"]
severity = config["SEVERITY"]
is_active = config["IS_ACTIVE"]
executed_by = config["CREATED_BY"]

source_table = f"{database}.{schema_name}.{table}"
df = session.table(source_table)

total_count = df.count()

print(f"\n{BOLD}================ DQ EXECUTION STARTED ================ {RESET}")
print(f"{BOLD}Table Checked : {table}{RESET}")
print(f"{BOLD}Total Records : {total_count}{RESET}\n")


# <<<<<<<<<<<<<<<<<<<<<< COLUMN LEVEL RULES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

for col_config in config.get("columns", []):

    column_name = col_config["column"]

    for rule in col_config["rules"]:

        start_time = datetime.now()

        rule_id = rule["rule_id"]
        rule_type = rule_lookup.get(rule_id, rule.get("rule", "UNKNOWN"))
        threshold = float(rule.get("threshold", 0))

        if rule_type == "NULL_CHECK":

            failed_df = df.filter(col(column_name).is_null())
            rule_expression = f"{column_name} IS NOT NULL"

        elif rule_type == "RANGE_CHECK":

            min_val = rule["min_value"]
            max_val = rule["max_value"]

            failed_df = df.filter(
                (col(column_name) < min_val) |
                (col(column_name) > max_val)
            )

            rule_expression = f"{column_name} BETWEEN {min_val} AND {max_val}"

        elif rule_type == "MIN_LENGTH_CHECK":

            min_length = int(rule["min_length"])

            failed_df = df.filter(
                col(column_name).is_not_null() &
                (length(col(column_name)) < min_length)
            )

            rule_expression = f"LENGTH({column_name}) >= {min_length}"

        else:
            continue

        failed_count = failed_df.count()
        passed_count = total_count - failed_count

        failure_percentage = (
            failed_count / total_count if total_count > 0 else 0
        )

        threshold_breached = failure_percentage > threshold
        rule_status = "FAIL" if threshold_breached else "PASS"

        failed_sample = failed_df.limit(5).collect()
        failed_sample_json = [row.as_dict() for row in failed_sample]

        # 🎨 Choose color
        color = RED if rule_status == "FAIL" else GREEN

        print("-------------------------------------------")
        print(f"Rule ID         : {rule_id}")
        print(f"Rule Type       : {rule_type}")
        print(f"Column          : {column_name}")

        if rule_type == "RANGE_CHECK":
            print(f"Range Allowed   : {min_val} to {max_val}")

        elif rule_type == "MIN_LENGTH_CHECK":
            print(f"Min Length Req  : {min_length}")

        print(f"Total Records   : {total_count}")
        print(f"Failed Records  : {failed_count}")
        print(f"Failure %       : {round(failure_percentage,4)}")
        print(f"Threshold       : {threshold}")
        print(f"Status          : {color}{rule_status}{RESET}")
        print("-------------------------------------------\n")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        query_id = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
        warehouse = session.get_current_warehouse()

        row_dict = {
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
            "IS_ACTIVE": is_active,
            "EXECUTED_BY": executed_by,
            "EXECUTION_MODE": "SNOWPARK",
            "CREATED_TIMESTAMP": datetime.now(),
            "UPDATED_TIMESTAMP": datetime.now()
        }

        session.create_dataframe([row_dict]) \
               .write.mode("append") \
               .save_as_table("DEMO_DB.PUBLIC.DQ_RESULT_TABLE")


# <<<<<<<<<<<<<<<<<<<<<< TABLE LEVEL RULES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

for table_rule in config.get("table_rules", []):

    start_time = datetime.now()

    rule_id = table_rule["rule_id"]
    rule_type = table_rule["rule"]
    threshold = float(table_rule["threshold"])
    col_list = table_rule["columns"]

    dup_df = (
        df.group_by([col(c) for c in col_list])
          .count()
          .filter(col("COUNT") > 1)
    )

    duplicate_groups = dup_df.count()
    passed_count = total_count - duplicate_groups

    failure_percentage = (
        duplicate_groups / total_count if total_count > 0 else 0
    )

    threshold_breached = failure_percentage > threshold
    rule_status = "FAIL" if threshold_breached else "PASS"

    failed_sample = dup_df.limit(5).collect()
    failed_sample_json = [row.as_dict() for row in failed_sample]

    error_message = None
    if threshold_breached:
        error_message = {
            "reason": "DUPLICATE_THRESHOLD_BREACH",
            "columns_checked": col_list,
            "duplicate_groups": duplicate_groups,
            "total_count": total_count,
            "failure_percentage": round(failure_percentage, 4),
            "threshold_allowed": threshold
        }

    color = RED if rule_status == "FAIL" else GREEN

    print("===========================================")
    print(f"Rule ID         : {rule_id}")
    print(f"Rule Type       : {rule_type}")
    print(f"Columns Checked : {col_list}")
    print(f"Duplicate Groups: {duplicate_groups}")
    print(f"Failure %       : {round(failure_percentage,4)}")
    print(f"Threshold       : {threshold}")
    print(f"Status          : {color}{rule_status}{RESET}")
    print("===========================================\n")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    query_id = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
    warehouse = session.get_current_warehouse()

    row_dict = {
        "RULE_ID": rule_id,
        "RULE_TYPE": rule_type,
        "DATABASE_NAME": database,
        "SCHEMA_NAME": schema_name,
        "TABLE_NAME": table,
        "COLUMN_NAME": ",".join(col_list),
        "RULE_EXPRESSION": f"DUPLICATE CHECK ON ({', '.join(col_list)})",
        "THRESHOLD": threshold,
        "SEVERITY": severity,
        "TOTAL_RECORD_COUNT": total_count,
        "FAILED_RECORD_COUNT": duplicate_groups,
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
        "ERROR_MESSAGE": error_message,
        "IS_ACTIVE": is_active,
        "EXECUTED_BY": executed_by,
        "EXECUTION_MODE": "SNOWPARK",
        "CREATED_TIMESTAMP": datetime.now(),
        "UPDATED_TIMESTAMP": datetime.now()
    }

    session.create_dataframe([row_dict]) \
           .write.mode("append") \
           .save_as_table("DEMO_DB.PUBLIC.DQ_RESULT_TABLE")

print(f"{BOLD}✅ DQ EXECUTION COMPLETED SUCCESSFULLY{RESET}\n")
