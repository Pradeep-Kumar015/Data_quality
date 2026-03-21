from datetime import datetime
import json

from src.utils.logger import get_logger
import os
from src.alerts.http_alert import HTTPAlert

logger = get_logger(__name__)


class ReportGenerator:
    
    

    def __init__(self, session, teams_webhook=None):
        self.session = session
        self.teams_webhook = teams_webhook  # (not used now, safe)

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
        
        print("🔥 ENTERED generate_report")

        end_time = datetime.now()

        # ✅ Ensure threshold is float
        try:
            threshold = float(threshold) if threshold is not None else 0.0
        except Exception:
            threshold = 0.0

        # ✅ Calculations
        passed_count = total_count - failed_count

        failure_percentage = (
            failed_count / total_count if total_count > 0 else 0.0
        )

        is_threshold_breached = failure_percentage > threshold

        rule_status = "FAIL" if is_threshold_breached else "PASS"

        execution_duration = int((end_time - start_time).total_seconds())

        # ✅ Handle VARIANT column safely
        try:
            failed_sample = failed_df.limit(5).collect()
            failed_sample_json = [r.as_dict() for r in failed_sample]
        except Exception as e:
            print("❌ SAMPLE ERROR:", e)
            failed_sample_json = []

        # ✅ Build row (MATCHES YOUR TABLE STRUCTURE)
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

        # ✅ Insert into Snowflake
        try:
            df = self.session.create_dataframe([result_row])
            
            print("🔥 BEFORE INSERT")

            df = self.session.create_dataframe([result_row])

            print("🔥 DF CREATED")


            df.write.mode("append").save_as_table(
                "DEMO_DB.PUBLIC.DQ_RESULT_TABLE",
                column_order="name"
            )
            
            print("🔥 AFTER INSERT")

            logger.info(f"INSERT SUCCESS → {rule_id}")

        except Exception as e:
            logger.error(f"INSERT FAILED → {str(e)}")
            raise

        # ✅ Log result
        logger.info(f"{table}.{column_name} | {rule_type} | {rule_status}")

        return rule_status