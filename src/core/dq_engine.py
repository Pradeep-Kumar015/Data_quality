from datetime import datetime
from collections import defaultdict
import os
import pandas as pd
from pandas.errors import EmptyDataError
from snowflake.snowpark.functions import col, current_date, dateadd, lit
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DQEngine:

    def __init__(self, session, rule_lookup, teams_webhook=None):

        self.session = session
        self.rule_lookup = rule_lookup

        from src.reporting.report_generator import ReportGenerator
        self.reporter = ReportGenerator(session, teams_webhook)

    # =================================================
    # ✅ CHECK IF ALREADY PROCESSED
    # =================================================
    def is_already_processed(
            self,
            database,
            schema,
            table,
            rule_id,
            config_id,
            column_name
        ):

        log_file = "logs/dq_execution_log.csv"

        # File doesn't exist
        if not os.path.exists(log_file):
            return False

        try:
            log_df = pd.read_csv(log_file)

        # File exists but is empty
        except EmptyDataError:
            return False

        result = log_df[
            (log_df["DATABASE_NAME"] == database) &
            (log_df["SCHEMA_NAME"] == schema) &
            (log_df["TABLE_NAME"] == table) &
            (log_df["RULE_ID"] == rule_id) &
            (log_df["COLUMN_NAME"] == column_name) &
            (log_df["STATUS"] == "PASS")
        ]

        return len(result) > 0

    # =================================================
    # ✅ LOG EXECUTION (ONLY EXECUTION STATUS)
    # =================================================
    def log_execution(self, database, schema, table, rule_id, config_id, column_name, status):

        log_file = "logs/dq_execution_log.csv"

        row = {
            "DATABASE_NAME": database,
            "SCHEMA_NAME": schema,
            "TABLE_NAME": table,
            "RULE_ID": rule_id,
            "CONFIG_ID": config_id,
            "COLUMN_NAME": column_name,
            "RUN_DATE": str(datetime.now().date()),
            "STATUS": status,
            "LAST_RUN_TIME": str(datetime.now())
        }

        df = pd.DataFrame([row])

        os.makedirs("logs", exist_ok=True)

        if os.path.exists(log_file):
            write_header = (
                not os.path.exists(log_file)
                or os.path.getsize(log_file) == 0
            )
            df.to_csv(log_file, mode="a", header=write_header, index=False)
        else:
            df.to_csv(log_file, index=False)

    # =================================================
    # 🚀 MAIN EXECUTION
    # =================================================
    def execute(self, dq_config_df):

        tables_checked = 0
        rules_executed = 0
        pass_count = 0
        fail_count = 0
        critical_fail_count = 0

        processed_tables = {}
        grouped_rules = defaultdict(list)

        rows = dq_config_df

        # =================================================
        # STEP 1: GROUP RULES
        # =================================================
        for row in rows:

            row_dict = {k.upper(): v for k, v in row.items()}

            if "COLUMN_NAMES" in row_dict:
                row_dict["COLUMN_NAME"] = row_dict["COLUMN_NAMES"]

            rule_id = row_dict.get("RULE_ID")
            config_id = row_dict.get("CONFIG_ID")
            column_name = row_dict.get("COLUMN_NAME")

            if rule_id in ["DQ_001", "DQ_003", "DQ_004", "DQ_005"] and not column_name:
                raise ValueError(f"COLUMN_NAME missing: {row_dict}")

            key = (
                row_dict["DATABASE_NAME"],
                row_dict["SCHEMA_NAME"],
                row_dict["TABLE_NAME"],
                rule_id,
                config_id
            )
            

            grouped_rules[key].append(row_dict)

        # =================================================
        # STEP 2: EXECUTE RULES
        # =================================================
        for (database, schema, table, rule_id, config_id), rule_rows in grouped_rules.items():

            full_table_name = f"{database}.{schema}.{table}"

            # -----------------------------------------
            # LOAD TABLE ONCE
            # -----------------------------------------
            if full_table_name not in processed_tables:

                logger.info(f"Processing table: {full_table_name}")

                df = self.session.table(full_table_name)
                total_count = df.count()

                if total_count == 0:
                    logger.warning(f"No data in {full_table_name}, skipping")
                    continue

                partition_column = None
                for row in rule_rows:
                    if row.get("PARTITION_COLUMN"):
                        partition_column = row.get("PARTITION_COLUMN")
                        break

                if partition_column:
                    df_columns = {c.upper() for c in df.columns}
                    if partition_column.upper() in df_columns:
                        today_df = df.filter(
                            col(partition_column).cast("DATE") == current_date()
                        )
                        today_count = today_df.count()
                        if today_count > 0:
                            logger.info(
                                f"Current date rows found for {full_table_name}: {today_count}. Running checks on current date data."
                            )
                            df = today_df
                            total_count = today_count
                        else:
                            logger.info(
                                f"No current date rows for {full_table_name}. Running checks on all available data ({total_count} rows)."
                            )
                    else:
                        logger.warning(
                            f"{full_table_name} does not contain partition column {partition_column}; running checks on full table."
                        )

                processed_tables[full_table_name] = (df, total_count)
                tables_checked += 1
                logger.info(
                    f"Rows available for validation: {total_count}"
                )

            df, total_count = processed_tables[full_table_name]

            # -----------------------------------------
            # RULE META
            # -----------------------------------------
            rule_metadata = self.rule_lookup.get(rule_id)

            if not rule_metadata:
                logger.error(f"Rule metadata missing for {rule_id}")
                continue

            rule_func = rule_metadata["func"]
            rule_name = rule_metadata["name"]

            # =================================================
            # STEP 3: EXECUTE EACH RULE
            # =================================================
            for row_dict in rule_rows:

                column_name = row_dict.get("COLUMN_NAME")
                threshold = float(row_dict.get("THRESHOLD") or 0.0)
                severity = row_dict.get("SEVERITY", "LOW").upper()
                min_val = row_dict.get("MIN_VALUE")
                max_val = row_dict.get("MAX_VALUE")

                # -----------------------------------------
                # 🔥 UNIQUE KEY USING CONFIG_ID
                # -----------------------------------------
                if rule_id == "DQ_005":

                    config_id = row_dict.get("CONFIG_ID")

                    if not config_id:
                        raise ValueError(
                            f"CONFIG_ID missing for CUSTOM_SQL rule: {row_dict}"
                        )

                    column_key = f"CUSTOM_SQL_{config_id}"

                else:
                    column_key = column_name

                    if rule_id == "DQ_002" and not column_key:
                        key_columns = row_dict.get("KEY_COLUMNS")
                        if isinstance(key_columns, list):
                            column_key = ",".join(key_columns)
                        elif key_columns is not None:
                            column_key = str(key_columns)

                # -----------------------------------------
                # 🔥 SKIP IF ALREADY PROCESSED
                # -----------------------------------------
                if self.is_already_processed(
                    database, schema, table, rule_id, config_id, column_key
                ):
                    logger.info(
                        f"Skipping {rule_id} on {column_key} (already processed)"
                    )
                    continue

                start_time = datetime.now()
                query_id = None
                query_history = None

                try:
                    with self.session.query_history(True) as qh:
                        query_history = qh
                        column_name_for_report = column_key

                        # -----------------------------------------
                        # CUSTOM SQL
                        # -----------------------------------------
                        if rule_id == "DQ_005":

                            custom_sql = row_dict.get("CUSTOM_SQL")

                            if not custom_sql:
                                raise ValueError("CUSTOM_SQL missing")

                            failed_df, failed_count, rule_expression = \
                                rule_func(self.session, custom_sql)

                            column_name_for_report = column_key

                        # -----------------------------------------
                        # STANDARD RULE
                        # -----------------------------------------
                        else:

                            if rule_id == "DQ_002":
                                key_columns = row_dict.get("KEY_COLUMNS")
                                failed_df, failed_count, rule_expression = rule_func(
                                    df,
                                    column_name,
                                    key_columns
                                )

                            elif rule_id == "DQ_006":

                                valid_values = row_dict.get("VALID_VALUES")

                                failed_df, failed_count, rule_expression = rule_func(
                                    df,
                                    column_name,
                                    valid_values
                                )

                            else:

                                params = [column_name]

                                if min_val is not None:
                                    params.append(min_val)

                                if max_val is not None:
                                    params.append(max_val)

                                failed_df, failed_count, rule_expression = rule_func(
                                    df,
                                    *params
                                )

                        if query_history.queries:
                            query_id = query_history.queries[-1].query_id

                    # -----------------------------------------
                    # REPORT (DQ RESULT)
                    # -----------------------------------------
                    print(
                        f"Executing {rule_id}, CONFIG_ID={config_id}, "
                        f"COLUMN={column_name_for_report}"
                    )
                    rule_status = self.reporter.generate_report(
                        rule_id=rule_id,
                        rule_type=rule_name,
                        database=database,
                        schema=schema,
                        table=table,
                        config_id=config_id,
                        column_name=column_name_for_report,
                        rule_expression=rule_expression,
                        threshold=threshold,
                        severity=severity,
                        total_count=total_count,
                        failed_count=failed_count,
                        start_time=start_time,
                        executed_by="DQ_TOOL",
                        source_table=full_table_name,
                        failed_df=failed_df,
                        query_id=query_id
                    )
                    print(
                        f"Completed {rule_id}, CONFIG_ID={config_id}"
                    )

                    # -----------------------------------------
                    # 🔥 EXECUTION STATUS LOGGING
                    # -----------------------------------------
                    execution_status = (
                        "PASS" if rule_status == "PASS" else "FAIL"
                    )
                    self.log_execution(
                        database,
                        schema,
                        table,
                        rule_id,
                        config_id,
                        column_key,
                        execution_status
                    )

                    rules_executed += 1

                    # DQ metrics (separate from execution)
                    if rule_status == "PASS":
                        pass_count += 1
                    else:
                        fail_count += 1
                        if severity == "HIGH":
                            critical_fail_count += 1

                except Exception as e:
                    if query_history and query_history.queries:
                        query_id = query_history.queries[-1].query_id

                    logger.error(
                        f"Error in {rule_id} on {column_key}: {str(e)}"
                    )

                    self.log_execution(
                        database,
                        schema,
                        table,
                        rule_id,
                        config_id,
                        column_key,
                        "FAIL"
                    )
                    self.reporter.generate_error_report(
                        rule_id=rule_id,
                        rule_type=rule_name,
                        database=database,
                        schema=schema,
                        table=table,
                        config_id=config_id,
                        column_name=column_key,
                        rule_expression=locals().get("rule_expression", ""),
                        threshold=threshold,
                        severity=severity,
                        total_count=locals().get("total_count", 0),
                        failed_count=0,
                        start_time=locals().get("start_time", datetime.now()),
                        executed_by="DQ_TOOL",
                        source_table=full_table_name,
                        failed_df=locals().get("failed_df", df.filter("1=0") if "df" in locals() else None),
                        error_message=str(e),
                        query_id=query_id
                    )

                    rules_executed += 1
                    fail_count += 1

                    continue

        return (
            tables_checked,
            rules_executed,
            pass_count,
            fail_count,
            critical_fail_count
        )