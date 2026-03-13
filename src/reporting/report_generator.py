from datetime import datetime

from alerts.teams_alert import TeamsAlert
from utils.logger import get_logger

logger = get_logger(__name__)


class ReportGenerator:

    def __init__(self, session, teams_webhook):

        self.session = session
        self.teams_alert = TeamsAlert(teams_webhook)


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

        passed_count = total_count - failed_count

        failure_percentage = (
            failed_count / total_count if total_count > 0 else 0
        )

        rule_status = "FAIL" if failure_percentage > threshold else "PASS"

        execution_duration = (end_time - start_time).total_seconds()

        failed_sample = failed_df.limit(5).collect()
        failed_sample_json = [r.as_dict() for r in failed_sample]

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
            "FAILED_SAMPLE_DATA": failed_sample_json,
            "START_TIME": start_time,
            "END_TIME": end_time,
            "EXECUTION_DURATION_SEC": execution_duration
        }

        self.session.create_dataframe([result_row]) \
            .write.mode("append") \
            .save_as_table("DEMO_DB.PUBLIC.DQ_RESULT_TABLE")

        logger.info(
            f"{table}.{column_name} | {rule_type} | {rule_status}"
        )

        # Teams alert for failures
        if rule_status == "FAIL":

            self.teams_alert.send_failure_alert(
                table,
                column_name,
                rule_type,
                failure_percentage,
                threshold
            )

        return rule_status