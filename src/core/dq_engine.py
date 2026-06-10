# from datetime import datetime
# from collections import defaultdict
# from snowflake.snowpark.functions import col, current_date, dateadd, lit

# from src.utils.logger import get_logger

# logger = get_logger(__name__)


# class DQEngine:

#     def __init__(self, session, rule_lookup, teams_webhook=None):

#         self.session = session
#         self.rule_lookup = rule_lookup

#         from src.reporting.report_generator import ReportGenerator
#         self.reporter = ReportGenerator(session, teams_webhook)


#     def execute(self, dq_config_df):

#         tables_checked = 0
#         rules_executed = 0
#         pass_count = 0
#         fail_count = 0
#         critical_fail_count = 0

#         processed_tables = {}
#         grouped_rules = defaultdict(list)

#         rows = dq_config_df.collect()

#         # =================================================
#         # STEP 1: GROUP RULES BY TABLE + RULE_ID
#         # =================================================
#         for row in rows:

#             row_dict = {
#                 k.upper(): v
#                 for k, v in row.as_dict().items()
#             }

#             if "COLUMN_NAMES" in row_dict:
#                 row_dict["COLUMN_NAME"] = row_dict["COLUMN_NAMES"]

#             rule_id = row_dict.get("RULE_ID")
#             column_name = row_dict.get("COLUMN_NAME")

#             if rule_id != "DQ_005" and not column_name:

#                 raise ValueError(
#                     f"COLUMN_NAME missing in config: {row_dict}"
#                 )

#             key = (
#                 row_dict["DATABASE_NAME"],
#                 row_dict["SCHEMA_NAME"],
#                 row_dict["TABLE_NAME"],
#                 rule_id
#             )

#             grouped_rules[key].append(row_dict)


#         # =================================================
#         # STEP 2: EXECUTE GROUPED RULES
#         # =================================================
#         for (
#             database,
#             schema,
#             table,
#             rule_id
#         ), rule_rows in grouped_rules.items():

#             full_table_name = f"{database}.{schema}.{table}"

#             # -------------------------------------------------
#             # LOAD TABLE ONLY ONCE
#             # -------------------------------------------------
#             if full_table_name not in processed_tables:

#                 logger.info(f"Processing table: {full_table_name}")

#                 df = self.session.table(full_table_name)

#                 partition_column = rule_rows[0].get("PARTITION_COLUMN")

#                 if partition_column:

#                     if partition_column not in df.columns:

#                         logger.warning(
#                             f"{partition_column} not found in "
#                             f"{full_table_name}. Skipping partition filter."
#                         )

#                     else:

#                         logger.info(
#                             f"Filtering {full_table_name} using "
#                             f"{partition_column} = CURRENT_DATE"
#                         )

#                         # df = df.filter(
#                         #     col(partition_column) == current_date()
#                         # )

#                         df = df.filter(
#                                 (col(partition_column) >= current_date()) &
#                                 (col(partition_column) < dateadd("day", lit(1), current_date()))
#                             )

#                 total_count = df.count()

#                 if total_count == 0:

#                     logger.warning(
#                         f"No records available for validation in "
#                         f"{full_table_name}. Skipping table."
#                     )

#                     continue

#                 processed_tables[full_table_name] = (
#                     df,
#                     total_count
#                 )

#                 tables_checked += 1

#                 logger.info(
#                     f"Rows available for validation: {total_count}"
#                 )

#             df, total_count = processed_tables[full_table_name]

#             # =================================================
#             # LOAD RULE METADATA (func + name)
#             # =================================================
#             rule_metadata = self.rule_lookup.get(rule_id)

#             if not rule_metadata:

#                 logger.error(
#                     f"Rule metadata missing for {rule_id}"
#                 )

#                 continue

#             rule_func = rule_metadata["func"]
#             rule_name = rule_metadata["name"]

#             logger.info(
#                 f"Executing grouped rule {rule_name} on table {table}"
#             )

#             # =================================================
#             # STEP 3: EXECUTE RULES
#             # =================================================
#             for row_dict in rule_rows:

#                 column_name = row_dict.get("COLUMN_NAME")

#                 threshold = float(
#                     row_dict.get("THRESHOLD") or 0.0
#                 )

#                 severity = (
#                     row_dict.get("SEVERITY", "LOW").upper()
#                 )

#                 min_val = row_dict.get("MIN_VALUE")
#                 max_val = row_dict.get("MAX_VALUE")

#                 start_time = datetime.now()

#                 try:

#                     # -----------------------------------------
#                     # CUSTOM SQL RULE EXECUTION
#                     # -----------------------------------------
#                     if rule_id == "DQ_005":

#                         custom_sql = row_dict.get("CUSTOM_SQL")

#                         if not custom_sql:

#                             raise ValueError(
#                                 f"CUSTOM_SQL missing for rule {rule_id}"
#                             )

#                         logger.info(
#                             f"Executing CUSTOM SQL rule on {table}"
#                         )

#                         failed_df, failed_count, rule_expression = \
#                             rule_func(self.session, custom_sql)

#                         column_name_for_report = "CUSTOM_SQL"

#                     # -----------------------------------------
#                     # STANDARD COLUMN RULE EXECUTION
#                     # -----------------------------------------
#                     else:

#                         params = [column_name]

#                         if min_val is not None:
#                             params.append(min_val)

#                         if max_val is not None:
#                             params.append(max_val)

#                         failed_df, failed_count, rule_expression = \
#                             rule_func(df, *params)

#                         column_name_for_report = column_name

#                     # -----------------------------------------
#                     # WRITE RESULT
#                     # -----------------------------------------
#                     rule_status = self.reporter.generate_report(
#                         rule_id=rule_id,
#                         rule_type=rule_name,   # ✅ FIXED HERE
#                         database=database,
#                         schema=schema,
#                         table=table,
#                         column_name=column_name_for_report,
#                         rule_expression=rule_expression,
#                         threshold=threshold,
#                         severity=severity,
#                         total_count=total_count,
#                         failed_count=failed_count,
#                         start_time=start_time,
#                         executed_by="DQ_TOOL",
#                         source_table=full_table_name,
#                         failed_df=failed_df
#                     )

#                     rules_executed += 1

#                     if rule_status == "PASS":

#                         pass_count += 1

#                     else:

#                         fail_count += 1

#                         if severity == "HIGH":

#                             critical_fail_count += 1

#                 except Exception as e:

#                     logger.error(
#                         f"Error executing {rule_name} on "
#                         f"{column_name}: {str(e)}"
#                     )

#                     rules_executed += 1
#                     fail_count += 1


#         # =================================================
#         # STEP 4: RETURN SUMMARY METRICS
#         # =================================================
#         return (
#             tables_checked,
#             rules_executed,
#             pass_count,
#             fail_count,
#             critical_fail_count
#         )


from datetime import datetime
from collections import defaultdict

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
    def is_already_processed(self, database, schema, table, rule_id, config_id, column_name):

        query = f"""
            SELECT 1
            FROM DEMO_DB.PUBLIC.DQ_EXECUTION_LOG
            WHERE DATABASE_NAME = '{database}'
              AND SCHEMA_NAME = '{schema}'
              AND TABLE_NAME = '{table}'
              AND RULE_ID = '{rule_id}'
              AND CONFIG_ID = '{config_id}'  -- IMPORTANT: CHECK CONFIG_ID FOR CUSTOM SQL
              AND COLUMN_NAME = '{column_name}'
              AND RUN_DATE = CURRENT_DATE()
              AND STATUS = 'PASS'
            LIMIT 1
        """

        return len(self.session.sql(query).collect()) > 0

    # =================================================
    # ✅ LOG EXECUTION (ONLY EXECUTION STATUS)
    # =================================================
    def log_execution(self, database, schema, table, rule_id, config_id, column_name, status):

        # Handle NULL CONFIG_ID
        config_id_value = "NULL" if config_id is None else config_id

        insert_sql = f"""
            INSERT INTO DEMO_DB.PUBLIC.DQ_EXECUTION_LOG
            (
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                RULE_ID,
                CONFIG_ID,
                COLUMN_NAME,
                RUN_DATE,
                STATUS,
                LAST_RUN_TIME
            )
            SELECT
                '{database}',
                '{schema}',
                '{table}',
                '{rule_id}',
                {config_id_value},
                '{column_name}',
                CURRENT_DATE(),
                '{status}',
                CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM DEMO_DB.PUBLIC.DQ_EXECUTION_LOG d
                WHERE d.DATABASE_NAME = '{database}'
                AND d.SCHEMA_NAME = '{schema}'
                AND d.TABLE_NAME = '{table}'
                AND d.RULE_ID = '{rule_id}'
                AND (
                        d.CONFIG_ID = {config_id_value}
                        OR ({config_id_value} IS NULL AND d.CONFIG_ID IS NULL)
                    )
                AND d.COLUMN_NAME = '{column_name}'
                AND d.RUN_DATE = CURRENT_DATE()
                AND d.STATUS = 'PASS'
            )
        """

        self.session.sql(insert_sql).collect()

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

        rows = dq_config_df.collect()

        # =================================================
        # STEP 1: GROUP RULES
        # =================================================
        for row in rows:

            row_dict = {k.upper(): v for k, v in row.as_dict().items()}

            if "COLUMN_NAMES" in row_dict:
                row_dict["COLUMN_NAME"] = row_dict["COLUMN_NAMES"]

            rule_id = row_dict.get("RULE_ID")
            config_id = row_dict.get("CONFIG_ID")
            column_name = row_dict.get("COLUMN_NAME")

            if rule_id != "DQ_005" and not column_name:
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

                partition_column = rule_rows[0].get("PARTITION_COLUMN")

                if partition_column and partition_column in df.columns:

                    df = df.filter(
                        (col(partition_column) >= current_date()) &
                        (col(partition_column) < dateadd("day", lit(1), current_date()))
                    )

                total_count = df.count()

                if total_count == 0:
                    logger.warning(f"No data in {full_table_name}, skipping")
                    continue

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

                try:

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

                        params = [column_name]

                        if min_val is not None:
                            params.append(min_val)

                        if max_val is not None:
                            params.append(max_val)

                        failed_df, failed_count, rule_expression = \
                            rule_func(df, *params)

                        column_name_for_report = column_name

                    # -----------------------------------------
                    # REPORT (DQ RESULT)
                    # -----------------------------------------
                    rule_status = self.reporter.generate_report(
                        rule_id=rule_id,
                        rule_type=rule_name,
                        database=database,
                        schema=schema,
                        table=table,
                        column_name=column_name_for_report,
                        rule_expression=rule_expression,
                        threshold=threshold,
                        severity=severity,
                        total_count=total_count,
                        failed_count=failed_count,
                        start_time=start_time,
                        executed_by="DQ_TOOL",
                        source_table=full_table_name,
                        failed_df=failed_df
                    )

                    # -----------------------------------------
                    # 🔥 EXECUTION SUCCESS (IMPORTANT FIX)
                    # -----------------------------------------
                    self.log_execution(
                        database,
                        schema,
                        table,
                        rule_id,
                        config_id,
                        column_key,
                        "PASS"
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