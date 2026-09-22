import json
from datetime import datetime, date

from src.alerts.teams_alert import TeamsAlert
from src.utils.logger import get_logger


logger = get_logger(__name__)


class ReportGenerator:

    # ============================================================
    # RESULT TABLE
    # ============================================================

    RESULT_TABLE = "BI_DATA_QUALITY_UAT.DQT.DQ_RESULT"

    # ============================================================
    # INITIALIZATION
    # ============================================================

    def __init__(self, session, teams_webhook=None):

        self.session = session

        self.teams_alert = (
            TeamsAlert(teams_webhook)
            if teams_webhook
            else None
        )

    # ============================================================
    # FAILED SAMPLE DATA
    # ============================================================

    def _get_failed_sample_json(self, failed_df):
        """
        Extract up to 5 failed records and convert them
        into a JSON string.

        DQ_RESULT.FAILED_SAMPLE_DATA is VARCHAR, so the
        Python list must be serialized before inserting.
        """

        failed_sample_json = "[]"

        try:

            if failed_df is not None:

                failed_sample_rows = (
                    failed_df
                    .limit(5)
                    .collect()
                )

                def serialize_value(value):

                    if isinstance(
                        value,
                        (date, datetime)
                    ):
                        return value.isoformat()

                    return value

                def serialize_row(row):

                    return {
                        str(key): serialize_value(value)
                        for key, value
                        in row.as_dict().items()
                    }

                failed_sample_data = [
                    serialize_row(row)
                    for row in failed_sample_rows
                ]

                # IMPORTANT:
                # FAILED_SAMPLE_DATA is VARCHAR in Snowflake.
                # Convert the Python list to a JSON string.
                failed_sample_json = json.dumps(
                    failed_sample_data,
                    default=str
                )

        except Exception as exc:

            logger.warning(
                "Failed sample extraction error: "
                f"{exc}"
            )

            failed_sample_json = "[]"

        return failed_sample_json

    # ============================================================
    # GENERATE REPORT
    # ============================================================

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
        query_id=None
    ):

        execution_timestamp = datetime.now()

        # --------------------------------------------------------
        # Normalize values
        # --------------------------------------------------------

        severity = (
            severity or "LOW"
        ).upper()

        threshold = float(
            threshold or 0.0
        )

        total_count = int(
            total_count or 0
        )

        failed_count = int(
            failed_count or 0
        )

        passed_count = max(
            total_count - failed_count,
            0
        )

        # --------------------------------------------------------
        # Failure percentage
        #
        # Example:
        #
        # 10 failed / 100 total = 0.10
        #
        # threshold = 0.00
        # failure_percentage = 0.10
        #
        # => FAIL
        # --------------------------------------------------------

        failure_percentage = (
            float(failed_count) / float(total_count)
            if total_count > 0
            else 0.0
        )

        # --------------------------------------------------------
        # Threshold check
        # --------------------------------------------------------

        is_threshold_breached = (
            failure_percentage > threshold
        )

        rule_status = (
            "FAIL"
            if is_threshold_breached
            else "PASS"
        )

        # --------------------------------------------------------
        # Execution duration
        # --------------------------------------------------------

        execution_duration = (
            execution_timestamp - start_time
        ).total_seconds()

        # ========================================================
        # FAILED SAMPLE DATA
        # ========================================================

        failed_sample_json = (
            self._get_failed_sample_json(failed_df)
        )

        # ========================================================
        # WAREHOUSE
        # ========================================================

        warehouse_name = None

        try:

            warehouse_result = self.session.sql(
                "SELECT CURRENT_WAREHOUSE() AS WAREHOUSE_NAME"
            ).collect()

            if warehouse_result:

                warehouse_name = (
                    warehouse_result[0]
                    ["WAREHOUSE_NAME"]
                )

        except Exception as exc:

            logger.warning(
                "Unable to determine current warehouse: "
                f"{exc}"
            )

        # ========================================================
        # PREPARE RESULT ROW
        # ========================================================

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

            "IS_THRESHOLD_BREACHED":
                is_threshold_breached,

            "START_TIME": start_time,

            "END_TIME":
                execution_timestamp,

            "EXECUTION_DURATION_SEC":
                execution_duration,

            "QUERY_ID": query_id,

            "WAREHOUSE_NAME":
                warehouse_name,

            "SOURCE_TYPE":
                "SNOWFLAKE",

            "SOURCE_LOCATION":
                source_table,

            # IMPORTANT:
            # This is now a JSON STRING, not a Python list.
            "FAILED_SAMPLE_DATA":
                failed_sample_json,

            "ERROR_MESSAGE":
                None,

            "IS_ACTIVE":
                True,

            "EXECUTED_BY":
                executed_by,

            "EXECUTION_MODE":
                "BATCH",

            "CREATED_TIMESTAMP":
                execution_timestamp,

            "UPDATED_TIMESTAMP":
                execution_timestamp
        }

        # ========================================================
        # INSERT INTO DQ_RESULT
        # ========================================================

        try:

            logger.info(
                f"Inserting DQ result into "
                f"{self.RESULT_TABLE}: "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_name}"
            )

            result_df = (
                self.session
                .create_dataframe([result_row])
            )

            (
                result_df.write
                .mode("append")
                .save_as_table(
                    self.RESULT_TABLE,
                    column_order="name"
                )
            )

            logger.info(
                f"DQ_RESULT insert successful: "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_name}, "
                f"STATUS={rule_status}"
            )

        except Exception as exc:

            logger.error(
                f"DQ_RESULT insert failed for "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_name}: "
                f"{exc}"
            )

            raise

        # ========================================================
        # TEAMS ALERT
        # ========================================================

        if rule_status == "FAIL":

            logger.warning(
                f"Rule FAILED → "
                f"{rule_id} | "
                f"{table}.{column_name} | "
                f"Severity={severity}"
            )

            if (
                severity == "HIGH"
                and self.teams_alert
            ):

                try:

                    self.teams_alert.send_failure_alert(
                        table,
                        column_name,
                        rule_type,
                        failure_percentage,
                        threshold
                    )

                    logger.info(
                        "Teams HIGH severity alert "
                        "sent successfully"
                    )

                except Exception as exc:

                    logger.error(
                        "Teams failure alert failed: "
                        f"{exc}"
                    )

        return rule_status

    # ============================================================
    # ERROR REPORT
    # ============================================================

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
        failed_df,
        error_message,
        query_id=None
    ):

        execution_timestamp = datetime.now()

        # --------------------------------------------------------
        # Normalize values
        # --------------------------------------------------------

        threshold = float(
            threshold or 0.0
        )

        total_count = int(
            total_count or 0
        )

        failed_count = int(
            failed_count or 0
        )

        passed_count = max(
            total_count - failed_count,
            0
        )

        # --------------------------------------------------------
        # Failure percentage
        # --------------------------------------------------------

        failure_percentage = (
            float(failed_count) / float(total_count)
            if total_count > 0
            else 0.0
        )

        # --------------------------------------------------------
        # Execution duration
        # --------------------------------------------------------

        execution_duration = (
            execution_timestamp - start_time
        ).total_seconds()

        # ========================================================
        # FAILED SAMPLE DATA
        # ========================================================

        failed_sample_json = (
            self._get_failed_sample_json(failed_df)
        )

        # ========================================================
        # WAREHOUSE
        # ========================================================

        warehouse_name = None

        try:

            warehouse_result = self.session.sql(
                "SELECT CURRENT_WAREHOUSE() AS WAREHOUSE_NAME"
            ).collect()

            if warehouse_result:

                warehouse_name = (
                    warehouse_result[0]
                    ["WAREHOUSE_NAME"]
                )

        except Exception as exc:

            logger.warning(
                "Unable to determine current warehouse: "
                f"{exc}"
            )

        # ========================================================
        # ERROR RESULT
        # ========================================================

        result_row = {

            "RULE_ID": rule_id,

            "RULE_TYPE": rule_type,

            "CONFIG_ID": config_id,

            "DATABASE_NAME": database,

            "SCHEMA_NAME": schema,

            "TABLE_NAME": table,

            "COLUMN_NAME": column_name,

            "RULE_EXPRESSION":
                rule_expression,

            "THRESHOLD":
                threshold,

            "SEVERITY":
                severity,

            "TOTAL_RECORD_COUNT":
                total_count,

            "FAILED_RECORD_COUNT":
                failed_count,

            "PASSED_RECORD_COUNT":
                passed_count,

            "FAILURE_PERCENTAGE":
                failure_percentage,

            "RULE_STATUS":
                "FAIL",

            "IS_THRESHOLD_BREACHED":
                False,

            "START_TIME":
                start_time,

            "END_TIME":
                execution_timestamp,

            "EXECUTION_DURATION_SEC":
                execution_duration,

            "QUERY_ID":
                query_id,

            "WAREHOUSE_NAME":
                warehouse_name,

            "SOURCE_TYPE":
                "SNOWFLAKE",

            "SOURCE_LOCATION":
                source_table,

            # IMPORTANT:
            # This is a JSON STRING, not a Python list.
            "FAILED_SAMPLE_DATA":
                failed_sample_json,

            "ERROR_MESSAGE":
                str(error_message),

            "IS_ACTIVE":
                True,

            "EXECUTED_BY":
                executed_by,

            "EXECUTION_MODE":
                "BATCH",

            "CREATED_TIMESTAMP":
                execution_timestamp,

            "UPDATED_TIMESTAMP":
                execution_timestamp
        }

        # ========================================================
        # INSERT ERROR RESULT
        # ========================================================

        try:

            logger.info(
                f"Inserting DQ error result into "
                f"{self.RESULT_TABLE}: "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}"
            )

            result_df = (
                self.session
                .create_dataframe([result_row])
            )

            (
                result_df.write
                .mode("append")
                .save_as_table(
                    self.RESULT_TABLE,
                    column_order="name"
                )
            )

            logger.info(
                f"DQ error result inserted successfully: "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}"
            )

        except Exception as exc:

            logger.error(
                f"Failed to insert DQ error result: "
                f"{exc}"
            )

            # Do not hide the original DQ error
            raise
