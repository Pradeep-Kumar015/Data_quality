from datetime import datetime, date
import os
import pandas as pd
import json
import re
from src.alerts.teams_alert import TeamsAlert
from src.utils.logger import get_logger
from decimal import Decimal

logger = get_logger(__name__)


class ReportGenerator:

    def __init__(self, session, teams_webhook=None):

        self.session = session
        warehouse_name = None
        if hasattr(self.session, "get_current_warehouse"):
            warehouse_name = self.session.get_current_warehouse()
            if isinstance(warehouse_name, str):
                warehouse_name = warehouse_name.strip().strip('"')
        self.warehouse_name = warehouse_name

        self.teams_alert = (
            TeamsAlert(teams_webhook)
            if teams_webhook
            else None
        )

    def _persist_result(self, result_file, result_df):
        os.makedirs("result", exist_ok=True)

        if os.path.exists(result_file):
            try:
                existing_header = pd.read_csv(result_file, nrows=0).columns.tolist()
            except Exception:
                existing_header = []

            new_columns = result_df.columns.tolist()
            if set(existing_header) != set(new_columns):
                existing_df = pd.read_csv(result_file)
                for col in new_columns:
                    if col not in existing_df.columns:
                        existing_df[col] = None
                existing_df = existing_df[new_columns]
                combined_df = pd.concat([existing_df, result_df], ignore_index=True)
                combined_df.to_csv(result_file, index=False)
                return

        write_header = not os.path.exists(result_file) or os.path.getsize(result_file) == 0
        result_df.to_csv(result_file, mode="a", header=write_header, index=False)

    def _normalize_error_message(self, error_message):
        if error_message is None:
            return None

        message = str(error_message).strip()
        if not message:
            return None

        numeric_match = re.search(
            r"(Numeric value\s+['\"].+?['\"]\s+is not recognized)",
            message,
            re.IGNORECASE
        )
        if numeric_match:
            return numeric_match.group(1)

        message = message.splitlines()[0].strip()
        sentence_end = re.search(r"[.!?]", message)
        if sentence_end:
            return message[:sentence_end.end()].strip()

        return message


    def generate_report(
        self,
        rule_id,
        rule_type,
        database,
        schema,
        table,
        config_id,
        column_name,
        rule_expression,
        threshold,
        severity,
        total_count,
        failed_count,
        start_time,
        executed_by,
        source_table,
        failed_df,
        error_message=None,
        query_id=None
    ):

        execution_timestamp = datetime.now()

        severity = (severity or "LOW").upper()

        threshold = float(threshold or 0.0)

        passed_count = max(total_count - failed_count, 0)

        failure_percentage = (
            float(failed_count) / float(total_count)
            if total_count > 0 else 0.0
        )

        is_threshold_breached = failure_percentage > threshold

        rule_status = "FAIL" if is_threshold_breached else "PASS"

        execution_duration = int(
            (execution_timestamp - start_time).total_seconds()
        )


        # --------------------------------------------------
        # Extract failed sample safely (Snowflake VARIANT safe)
        # --------------------------------------------------
        try:
            if failed_df is None:
                failed_sample_rows = []
            else:
                failed_sample_rows = failed_df.limit(5).collect()

            def serialize_row(row):
                return {
                    k: (
                        str(v)
                        if isinstance(v, (date, datetime, Decimal))
                        else v
                    )
                    for k, v in row.as_dict().items()
                }

            failed_sample_json = [
                serialize_row(row)
                for row in failed_sample_rows
            ]

        except Exception as e:

            logger.warning(
                f"Failed sample extraction error: {str(e)}"
            )

            failed_sample_json = []


        # --------------------------------------------------
        # Optional metadata capture (future-ready)
        # --------------------------------------------------
        warehouse_name = self.warehouse_name


        # --------------------------------------------------
        # Prepare result row
        # --------------------------------------------------
        result_row = {

            "RULE_ID": rule_id,
            "RULE_TYPE": rule_type,
            "CONFIG_ID": config_id,
            "DATABASE_NAME": database,
            "SCHEMA_NAME": schema,
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
            "IS_THRESHOLD_BREACHED": is_threshold_breached,
            "START_TIME": start_time,
            "END_TIME": execution_timestamp,
            "EXECUTION_DURATION_SEC": execution_duration,
            "QUERY_ID": query_id,
            "WAREHOUSE_NAME": warehouse_name,
            "SOURCE_TYPE": "SNOWFLAKE",
            "SOURCE_LOCATION": source_table,
            "FAILED_SAMPLE_DATA": json.dumps(failed_sample_json),
            "ERROR_MESSAGE": error_message,
            "IS_ACTIVE": True,
            "EXECUTED_BY": executed_by,
            "EXECUTION_MODE": "BATCH",
            "CREATED_TIMESTAMP": execution_timestamp,
            "UPDATED_TIMESTAMP": execution_timestamp
        }


        # --------------------------------------------------
        # Insert into Snowflake result csv
        # --------------------------------------------------
        try:
            print(f"START REPORT -> {rule_id} | {table} | {column_name}")

            result_df = pd.DataFrame([result_row])

            result_file = "result/dq_result.csv"

            os.makedirs("result", exist_ok=True)

            self._persist_result(result_file, result_df)
            print(f"END REPORT -> {rule_id} | {table} | {column_name}")

            logger.info(
                f"Result written successfully → {rule_id} ({column_name})"
            )
            
            #return rule_status

        except Exception as e:

            logger.error(
                f"Insert failed for {rule_id} ({column_name}): {str(e)}"
            )

            raise


        # --------------------------------------------------
        # Send Teams alert (ONLY HIGH severity failures)
        # --------------------------------------------------
        if rule_status == "FAIL":

            logger.warning(
                f"Rule FAILED → {rule_id} | {table}.{column_name} | Severity={severity}"
            )

            if severity == "HIGH" and self.teams_alert:

                try:

                    self.teams_alert.send_failure_alert(
                        table,
                        column_name,
                        rule_type,
                        failure_percentage,
                        threshold
                    )

                    logger.info(
                        "🚨 Teams HIGH severity alert sent successfully"
                    )

                except Exception as e:

                    logger.error(
                        f"Teams failure alert failed: {str(e)}"
                    )


        return rule_status

    def generate_error_report(
        self,
        rule_id,
        rule_type,
        database,
        schema,
        table,
        config_id,
        column_name,
        rule_expression,
        threshold,
        severity,
        total_count,
        failed_count,
        start_time,
        executed_by,
        source_table,
        failed_df=None,
        error_message=None,
        query_id=None
    ):

        execution_timestamp = datetime.now()
        severity = (severity or "LOW").upper()
        threshold = float(threshold or 0.0)
        passed_count = max(total_count - failed_count, 0)
        failure_percentage = (
            float(failed_count) / float(total_count)
            if total_count > 0 else 0.0
        )
        warehouse_name = self.warehouse_name
        result_row = {
            "RULE_ID": rule_id,
            "RULE_TYPE": rule_type,
            "CONFIG_ID": config_id,
            "DATABASE_NAME": database,
            "SCHEMA_NAME": schema,
            "TABLE_NAME": table,
            "COLUMN_NAME": column_name,
            "RULE_EXPRESSION": rule_expression,
            "THRESHOLD": threshold,
            "SEVERITY": severity,
            "TOTAL_RECORD_COUNT": total_count,
            "FAILED_RECORD_COUNT": failed_count,
            "PASSED_RECORD_COUNT": passed_count,
            "FAILURE_PERCENTAGE": failure_percentage,
            "RULE_STATUS": "FAIL",
            "IS_THRESHOLD_BREACHED": False,
            "START_TIME": start_time,
            "END_TIME": execution_timestamp,
            "EXECUTION_DURATION_SEC": int((execution_timestamp - start_time).total_seconds()),
            "QUERY_ID": query_id,
            "WAREHOUSE_NAME": warehouse_name,
            "SOURCE_TYPE": "SNOWFLAKE",
            "SOURCE_LOCATION": source_table,
            "FAILED_SAMPLE_DATA": json.dumps([]),
            "ERROR_MESSAGE": self._normalize_error_message(error_message),
            "IS_ACTIVE": True,
            "EXECUTED_BY": executed_by,
            "EXECUTION_MODE": "BATCH",
            "CREATED_TIMESTAMP": execution_timestamp,
            "UPDATED_TIMESTAMP": execution_timestamp
        }

        result_file = "result/dq_result.csv"
        self._persist_result(result_file, pd.DataFrame([result_row]))
        logger.info(
            f"Error result written successfully → {rule_id} ({column_name})"
        )

        if severity == "HIGH" and self.teams_alert:
            try:
                self.teams_alert.send_error_alert(
                    table,
                    column_name,
                    rule_type,
                    error_message
                )
                logger.info(
                    "🚨 Teams HIGH severity error alert sent successfully"
                )
            except Exception as e:
                logger.error(
                    f"Teams error alert failed: {str(e)}"
                )

        return "FAIL"