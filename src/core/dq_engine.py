from datetime import datetime
from collections import defaultdict
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

        # ✅ STEP 1: GROUP RULES
        grouped_rules = defaultdict(list)

        rows = dq_config_df.collect()

        for row in rows:
            row_dict = {k.upper(): v for k, v in row.as_dict().items()}

            if "COLUMN_NAMES" in row_dict:
                row_dict["COLUMN_NAME"] = row_dict["COLUMN_NAMES"]

            key = (
                row_dict.get("DATABASE_NAME"),
                row_dict.get("SCHEMA_NAME"),
                row_dict.get("TABLE_NAME"),
                row_dict.get("RULE_ID")
            )

            grouped_rules[key].append(row_dict)

        # ✅ STEP 2: PROCESS GROUPS
        for (database, schema, table, rule_id), rule_rows in grouped_rules.items():

            full_table_name = f"{database}.{schema}.{table}"

            if full_table_name not in processed_tables:
                logger.info(f"Processing table: {full_table_name}")

                df = self.session.table(full_table_name)
                total_count = df.count()

                processed_tables[full_table_name] = (df, total_count)
                tables_checked += 1

                logger.info(f"Total records: {total_count}")

            df, total_count = processed_tables[full_table_name]

            rule_func = self.rule_lookup.get(rule_id)

            if not callable(rule_func):
                raise TypeError(f"Rule function not callable for {rule_id}")

            logger.info(f"Executing grouped rule {rule_id} on table {table}")

            # ✅ Collect all columns for this rule
            columns = [r["COLUMN_NAME"] for r in rule_rows]

            start_time = datetime.now()

            # ✅ STEP 3: EXECUTE ONCE (loop columns but same df)
            for row_dict in rule_rows:

                column_name = row_dict["COLUMN_NAME"]

                try:
                    threshold = float(row_dict.get("THRESHOLD") or 0.0)
                except:
                    threshold = 0.0

                severity = row_dict.get("SEVERITY", "LOW")
                min_val = row_dict.get("MIN_VALUE")
                max_val = row_dict.get("MAX_VALUE")

                logger.info(f"→ Column: {column_name}")

                try:
                    # Execute rule
                    if rule_id == "DQ_003":
                        failed_df, failed_count, rule_expression = \
                            rule_func(df, column_name, min_val, max_val)

                    elif rule_id == "DQ_004":
                        failed_df, failed_count, rule_expression = \
                            rule_func(df, column_name, min_val)

                    else:
                        failed_df, failed_count, rule_expression = \
                            rule_func(df, column_name)

                    # Report per column
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
                    elif rule_status == "FAIL":
                        fail_count += 1

                except Exception as e:
                    logger.error(f"Error in {rule_id} for {column_name}: {str(e)}")
                    rules_executed += 1
                    fail_count += 1

        return tables_checked, rules_executed, pass_count, fail_count