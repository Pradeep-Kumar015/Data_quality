from datetime import datetime
from snowflake.snowpark.functions import col, current_date

from checks import completeness, uniqueness, validity
from reporting.report_generator import ReportGenerator
from utils.logger import get_logger


logger = get_logger(__name__)


class DQEngine:

    def __init__(self, session, rule_lookup):
        self.session = session
        self.rule_lookup = rule_lookup
        self.reporter = ReportGenerator(session)

    def execute(self, dq_config_df):

        table_groups = {}

        logger.info("Grouping rules by table")

        for row in dq_config_df.to_local_iterator():

            table_key = (
                row["DATABASE_NAME"],
                row["SCHEMA_NAME"],
                row["TABLE_NAME"]
            )

            table_groups.setdefault(table_key, []).append(row)

        for (database, schema_name, table), rules in table_groups.items():

            source_table = f"{database}.{schema_name}.{table}"

            logger.info(f"Processing table: {source_table}")

            df = self.session.table(source_table).filter(
                col("LOAD_DATE") == current_date()
            )

            total_count = df.count()

            logger.info(f"Total records for current day: {total_count}")

            if total_count == 0:
                logger.warning(f"No records found for table: {table}")
                continue

            for row in rules:

                start_time = datetime.now()

                rule_id = row["RULE_ID"]
                rule_type = self.rule_lookup.get(rule_id)

                column_name = row["COLUMN_NAMES"]
                min_val = row["MIN_VALUE"]
                max_val = row["MAX_VALUE"]
                threshold = float(row["THRESHOLD"])
                severity = row["SEVERITY"]
                executed_by = row["CREATED_BY"]

                logger.info(
                    f"Executing rule {rule_type} on column {column_name}"
                )

                if rule_type == "NULL_CHECK":

                    failed_df, failed_count, rule_expression = \
                        completeness.execute(df, column_name)

                elif rule_type == "DUPLICATE_CHECK":

                    failed_df, failed_count, rule_expression = \
                        uniqueness.execute(df, column_name)

                elif rule_type == "RANGE_CHECK":

                    failed_df, failed_count, rule_expression = \
                        validity.execute(df, column_name, min_val, max_val)

                else:
                    logger.error(f"Unsupported rule type: {rule_type}")
                    continue

                self.reporter.generate_report(
                    rule_id=rule_id,
                    rule_type=rule_type,
                    database=database,
                    schema=schema_name,
                    table=table,
                    column_name=column_name,
                    rule_expression=rule_expression,
                    threshold=threshold,
                    severity=severity,
                    total_count=total_count,
                    failed_count=failed_count,
                    start_time=start_time,
                    executed_by=executed_by,
                    source_table=source_table,
                    failed_df=failed_df
                )