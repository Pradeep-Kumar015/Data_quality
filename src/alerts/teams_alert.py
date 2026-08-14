import json
import os
import requests
from datetime import datetime

from src.utils.logger import get_logger

logger = get_logger(__name__)


class TeamsAlert:

    def __init__(self, webhook_url):

        if not webhook_url:
            raise ValueError(
                "Teams webhook URL is missing"
            )

        self.webhook_url = webhook_url


    # ---------------------------------------------------
    # FAILURE ALERT (Adaptive Card)
    # ---------------------------------------------------
    def send_failure_alert(
        self,
        table,
        column,
        rule,
        failure_percentage,
        threshold
    ):

        execution_time = datetime.now()

        airflow_context = self._get_airflow_context()

        payload = {

            "type": "message",
            "text": "Data Quality rule failure alert",

            "attachments": [

                {

                    "contentType":
                    "application/vnd.microsoft.card.adaptive",

                    "content": {

                        "$schema":
                        "http://adaptivecards.io/schemas/adaptive-card.json",

                        "type": "AdaptiveCard",

                        "version": "1.4",

                        "body": [

                            {

                                "type": "TextBlock",

                                "text":
                                "🚨 DATA QUALITY RULE FAILURE ALERT",

                                "weight": "Bolder",

                                "size": "Large",

                                "color": "Attention"
                            },

                            {

                                "type": "FactSet",

                                "facts": [

                                    {
                                        "title": "Table",
                                        "value": table
                                    },

                                    {
                                        "title": "Column",
                                        "value": column
                                    },

                                    {
                                        "title": "Rule",
                                        "value": rule
                                    },

                                    {
                                        "title": "Failure %",
                                        "value":
                                        f"{failure_percentage:.2%}"
                                    },

                                    {
                                        "title": "Threshold",
                                        "value":
                                        f"{threshold:.2%}"
                                    },

                                    {
                                        "title": "Execution Time",
                                        "value":
                                        str(execution_time)
                                    },

                                    *airflow_context
                                ]
                            },

                            {

                                "type": "TextBlock",

                                "text":
                                "— Data Quality Monitoring Framework",

                                "spacing": "Medium",

                                "isSubtle": True
                            }
                        ]
                    }
                }
            ]
        }

        payload["summary"] = (
            "Data Quality Rule Failure Alert: "
            f"{table}.{column}"
        )
        logger.debug(
            "Prepared Teams failure alert payload for %s.%s | rule=%s | failure_pct=%.4f | threshold=%.4f",
            table,
            column,
            rule,
            failure_percentage,
            threshold
        )
        self._send(payload)


    # ---------------------------------------------------
    # ERROR ALERT (Adaptive Card)
    # ---------------------------------------------------
    def send_error_alert(
        self,
        table,
        column,
        rule,
        error_message
    ):

        execution_time = datetime.now()
        airflow_context = self._get_airflow_context()
        payload = {
            "type": "message",
            "text": "Data Quality rule error alert",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema":
                        "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": "🚨 DATA QUALITY RULE ERROR ALERT",
                                "weight": "Bolder",
                                "size": "Large",
                                "color": "Attention"
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {
                                        "title": "Table",
                                        "value": table
                                    },
                                    {
                                        "title": "Column",
                                        "value": column
                                    },
                                    {
                                        "title": "Rule",
                                        "value": rule
                                    },
                                    {
                                        "title": "Error",
                                        "value": str(error_message)
                                    },
                                    {
                                        "title": "Execution Time",
                                        "value": str(execution_time)
                                    },
                                    *airflow_context
                                ]
                            },
                            {
                                "type": "TextBlock",
                                "text": "— Data Quality Monitoring Framework",
                                "spacing": "Medium",
                                "isSubtle": True
                            }
                        ]
                    }
                }
            ]
        }
        payload["summary"] = (
            "Data Quality Rule Error Alert: "
            f"{table}.{column}"
        )
        logger.debug(
            "Prepared Teams error alert payload for %s.%s | rule=%s | error=%s",
            table,
            column,
            rule,
            error_message
        )
        self._send(payload)


    # ---------------------------------------------------
    # SUMMARY ALERT (Adaptive Card)
    # ---------------------------------------------------
    def send_summary_alert(
        self,
        tables_checked,
        rules_executed,
        pass_count,
        fail_count
    ):

        execution_time = datetime.now()

        success_pct = (
            pass_count / rules_executed
            if rules_executed else 0
        )

        failure_pct = (
            fail_count / rules_executed
            if rules_executed else 0
        )

        status = (
            "✅ ALL RULES PASSED"
            if fail_count == 0
            else "⚠️ ACTION REQUIRED"
        )

        airflow_context = self._get_airflow_context()

        payload = {

            "type": "message",
            "text": "Data Quality execution summary",

            "attachments": [

                {

                    "contentType":
                    "application/vnd.microsoft.card.adaptive",

                    "content": {

                        "$schema":
                        "http://adaptivecards.io/schemas/adaptive-card.json",

                        "type": "AdaptiveCard",

                        "version": "1.4",

                        "body": [

                            {

                                "type": "TextBlock",

                                "text":
                                "📊 DATA QUALITY EXECUTION SUMMARY",

                                "weight": "Bolder",

                                "size": "Large"
                            },

                            {

                                "type": "TextBlock",

                                "text":
                                f"Status: {status}",

                                "weight": "Bolder",

                                "color":
                                "Good"
                                if fail_count == 0
                                else "Warning"
                            },

                            {

                                "type": "FactSet",

                                "facts": [

                                    {
                                        "title":
                                        "Tables Checked",
                                        "value":
                                        str(tables_checked)
                                    },

                                    {
                                        "title":
                                        "Rules Executed",
                                        "value":
                                        str(rules_executed)
                                    },

                                    {
                                        "title":
                                        "Rules Passed",
                                        "value":
                                        str(pass_count)
                                    },

                                    {
                                        "title":
                                        "Rules Failed",
                                        "value":
                                        str(fail_count)
                                    },

                                    {
                                        "title":
                                        "Success %",
                                        "value":
                                        f"{success_pct:.2%}"
                                    },

                                    {
                                        "title":
                                        "Failure %",
                                        "value":
                                        f"{failure_pct:.2%}"
                                    },

                                    {
                                        "title":
                                        "Execution Time",
                                        "value":
                                        str(execution_time)
                                    },

                                    *airflow_context
                                ]
                            },

                            {

                                "type": "TextBlock",

                                "text":
                                "— Data Quality Monitoring Framework",

                                "spacing": "Medium",

                                "isSubtle": True
                            }
                        ]
                    }
                }
            ]
        }

        logger.debug(
            "Prepared Teams summary alert payload | tables_checked=%s | rules_executed=%s | pass_count=%s | fail_count=%s",
            tables_checked,
            rules_executed,
            pass_count,
            fail_count
        )
        self._send(payload)


    # ---------------------------------------------------
    # INTERNAL SEND METHOD
    # ---------------------------------------------------
    def _send(self, payload):

        try:

            logger.debug(
                "Posting Teams webhook to %s",
                self.webhook_url
            )
            logger.debug(
                "Teams alert request payload: %s",
                json.dumps(payload, ensure_ascii=False)
            )

            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )

            try:
                response.raise_for_status()
            except Exception as http_err:
                logger.error(
                    "Teams alert HTTP failure: %s | status=%s | body=%s",
                    str(http_err),
                    response.status_code,
                    response.text
                )
                raise

            logger.info(
                "Teams alert sent successfully | status=%s",
                response.status_code
            )
            logger.debug(
                "Teams alert response body: %s",
                response.text
            )

        except Exception as e:

            logger.error(
                f"Teams alert failed: {str(e)}"
            )
            raise


    # ---------------------------------------------------
    # AIRFLOW CONTEXT SUPPORT
    # ---------------------------------------------------
    def _get_airflow_context(self):

        dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")
        task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
        execution_date = os.getenv(
            "AIRFLOW_CTX_EXECUTION_DATE"
        )

        context = []

        if dag_id:
            context.append({
                "title": "DAG ID",
                "value": dag_id
            })

        if task_id:
            context.append({
                "title": "Task ID",
                "value": task_id
            })

        if execution_date:
            context.append({
                "title": "Execution Date",
                "value": execution_date
            })

        return context