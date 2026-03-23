from datetime import datetime, date

from src.alerts.teams_alert import TeamsAlert
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ReportGenerator:

    def __init__(self, session, teams_webhook=None):

        self.session = session

        # Initialize Teams alert handler
        self.teams_alert = (
            TeamsAlert(teams_webhook)
            if teams_webhook
            else None
        )


    def generate_report(
        self,
        rule_id,
        rule_type,
        database,
        schema,
        table,
        column_name,
        rule_expression,
        threshold,
        severity,
        total_count,
        failed_count,
        start_time,
        executed_by,
        source_table,
        failed_df
    ):

        end_time = datetime.now()

        threshold = float(threshold or 0.0)

        passed_count = max(total_count - failed_count, 0)

        failure_percentage = (
            float(failed_count) / float(total_count)
            if total_count > 0 else 0.0
        )

        is_threshold_breached = failure_percentage > threshold

        rule_status = "FAIL" if is_threshold_breached else "PASS"

        execution_duration = int(
            (end_time - start_time).total_seconds()
        )


        # --------------------------------------------------
        # Extract failed sample safely (VARIANT compatible)
        # --------------------------------------------------
        try:

            failed_sample = failed_df.limit(5).collect()

            def serialize_row(row):

                serialized = {}

                for k, v in row.as_dict().items():

                    if isinstance(v, (date, datetime)):
                        serialized[k] = str(v)

                    else:
                        serialized[k] = v

                return serialized

            failed_sample_json = [
                serialize_row(row)
                for row in failed_sample
            ]

        except Exception as e:

            logger.warning(
                f"Failed sample extraction error: {str(e)}"
            )

            failed_sample_json = []


        # --------------------------------------------------
        # Prepare result row
        # --------------------------------------------------
        result_row = {

            "RULE_ID": rule_id,
            "RULE_TYPE": rule_type,
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
            "END_TIME": end_time,
            "EXECUTION_DURATION_SEC": execution_duration,
            "QUERY_ID": None,
            "WAREHOUSE_NAME": None,
            "SOURCE_TYPE": "SNOWFLAKE",
            "SOURCE_LOCATION": source_table,
            "FAILED_SAMPLE_DATA": failed_sample_json,
            "ERROR_MESSAGE": None,
            "IS_ACTIVE": True,
            "EXECUTED_BY": executed_by,
            "EXECUTION_MODE": "BATCH",
            "CREATED_TIMESTAMP": datetime.now(),
            "UPDATED_TIMESTAMP": datetime.now()
        }


        # --------------------------------------------------
        # Insert into Snowflake result table
        # --------------------------------------------------
        try:

            df = self.session.create_dataframe([result_row])

            df.write.mode("append").save_as_table(
                "DEMO_DB.PUBLIC.DQ_RESULT_TABLE",
                column_order="name"
            )

            logger.info(
                f"INSERT SUCCESS → {rule_id} ({column_name})"
            )

        except Exception as e:

            logger.error(
                f"Insert failed: {str(e)}"
            )

            raise


        # --------------------------------------------------
        # Send Teams failure alert (per rule)
        # --------------------------------------------------
        if rule_status == "FAIL" and self.teams_alert:

            try:

                self.teams_alert.send_failure_alert(
                    table,
                    column_name,
                    rule_type,
                    failure_percentage,
                    threshold
                )

            except Exception as e:

                logger.error(
                    f"Teams failure alert failed: {str(e)}"
                )


        return rule_status