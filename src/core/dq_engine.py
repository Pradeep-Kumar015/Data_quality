from datetime import datetime
from collections import defaultdict
from snowflake.snowpark.functions import col, current_date

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DQEngine:

    def __init__(self, session, rule_lookup, teams_webhook=None):

        self.session = session
        self.rule_lookup = rule_lookup

        from src.reporting.report_generator import ReportGenerator
        self.reporter = ReportGenerator(session, teams_webhook)


    def execute(self, dq_config_df):

        tables_checked = 0
        rules_executed = 0
        pass_count = 0
        fail_count = 0

        processed_tables = {}

        grouped_rules = defaultdict(list)

        rows = dq_config_df.collect()

        # =================================================
        # STEP 1: GROUP RULES BY TABLE + RULE_ID
        # =================================================
        for row in rows:

            row_dict = {
                k.upper(): v
                for k, v in row.as_dict().items()
            }

            # Normalize column name
            if "COLUMN_NAMES" in row_dict:
                row_dict["COLUMN_NAME"] = row_dict["COLUMN_NAMES"]

            column_name = row_dict.get("COLUMN_NAME")

            if not column_name:
                raise ValueError(
                    f"COLUMN_NAME missing in config: {row_dict}"
                )

            key = (
                row_dict["DATABASE_NAME"],
                row_dict["SCHEMA_NAME"],
                row_dict["TABLE_NAME"],
                row_dict["RULE_ID"]
            )

            grouped_rules[key].append(row_dict)


        # =================================================
        # STEP 2: EXECUTE GROUPED RULES
        # =================================================
        for (
            database,
            schema,
            table,
            rule_id
        ), rule_rows in grouped_rules.items():

            full_table_name = f"{database}.{schema}.{table}"

            # -------------------------------------------------
            # Load table only once
            # -------------------------------------------------
            if full_table_name not in processed_tables:

                logger.info(
                    f"Processing table: {full_table_name}"
                )

                df = self.session.table(full_table_name)

                partition_column = rule_rows[0].get(
                    "PARTITION_COLUMN"
                )

                # =========================================
                # OPTIONAL PARTITION FILTER (TODAY ONLY)
                # =========================================
                if partition_column:

                    if partition_column not in df.columns:

                        logger.warning(
                            f"{partition_column} not found in "
                            f"{full_table_name}. Skipping partition filter."
                        )

                    else:

                        logger.info(
                            f"Filtering {full_table_name} using "
                            f"{partition_column} = CURRENT_DATE"
                        )

                        df = df.filter(
                            col(partition_column) == current_date()
                        )

                total_count = df.count()

                if total_count == 0:

                    logger.warning(
                        f"No records available for validation in "
                        f"{full_table_name}. Skipping table."
                    )

                    continue

                processed_tables[full_table_name] = (
                    df,
                    total_count
                )

                tables_checked += 1

                logger.info(
                    f"Rows available for validation: {total_count}"
                )

            df, total_count = processed_tables[
                full_table_name
            ]

            rule_func = self.rule_lookup.get(rule_id)

            if not callable(rule_func):

                logger.error(
                    f"Rule function missing for {rule_id}"
                )

                continue

            logger.info(
                f"Executing grouped rule {rule_id} on table {table}"
            )

            # =================================================
            # STEP 3: EXECUTE RULE PER COLUMN
            # =================================================
            for row_dict in rule_rows:

                column_name = row_dict["COLUMN_NAME"]

                threshold = float(
                    row_dict.get("THRESHOLD") or 0.0
                )

                severity = row_dict.get("SEVERITY", "LOW")

                min_val = row_dict.get("MIN_VALUE")
                max_val = row_dict.get("MAX_VALUE")

                start_time = datetime.now()

                try:

                    # Dynamically pass parameters
                    params = [column_name]

                    if min_val is not None:
                        params.append(min_val)

                    if max_val is not None:
                        params.append(max_val)

                    failed_df, failed_count, rule_expression = \
                        rule_func(df, *params)

                    rule_status = self.reporter.generate_report(
                        rule_id=rule_id,
                        rule_type=rule_id,
                        database=database,
                        schema=schema,
                        table=table,
                        column_name=column_name,
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

                    rules_executed += 1

                    if rule_status == "PASS":
                        pass_count += 1
                    else:
                        fail_count += 1

                except Exception as e:

                    logger.error(
                        f"Error executing {rule_id} on "
                        f"{column_name}: {str(e)}"
                    )

                    rules_executed += 1
                    fail_count += 1


        # =================================================
        # STEP 4: RETURN SUMMARY METRICS
        # =================================================
        return (
            tables_checked,
            rules_executed,
            pass_count,
            fail_count
        )