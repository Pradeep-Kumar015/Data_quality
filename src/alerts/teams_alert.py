import requests
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TeamsAlert:

    def __init__(self, webhook_url):
        self.webhook_url = webhook_url


    # ---------------------------------------------------
    # FAILURE ALERT (Stylish Adaptive Card)
    # ---------------------------------------------------
    def send_failure_alert(
        self,
        table,
        column,
        rule,
        failure_percentage,
        threshold
    ):

        payload = {
            "type": "message",
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
                                        str(datetime.now())
                                    }
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

        self._send(payload)


    # ---------------------------------------------------
    # SUMMARY ALERT (Stylish Adaptive Card)
    # ---------------------------------------------------
    def send_summary_alert(
        self,
        tables_checked,
        rules_executed,
        pass_count,
        fail_count
    ):

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

        payload = {
            "type": "message",
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
                                "text": f"Status: {status}",
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
                                        str(datetime.now())
                                    }
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

        self._send(payload)


    # ---------------------------------------------------
    # INTERNAL SEND METHOD
    # ---------------------------------------------------
    def _send(self, payload):

        try:

            response = requests.post(
                self.webhook_url,
                json=payload
            )

            response.raise_for_status()

            logger.info(
                "Teams alert sent successfully"
            )

        except Exception as e:

            logger.error(
                f"Teams alert failed: {str(e)}"
            )