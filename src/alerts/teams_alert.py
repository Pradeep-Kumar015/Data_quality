import requests
from utils.logger import get_logger

logger = get_logger(__name__)


class TeamsAlert:

    def __init__(self, webhook_url):
        self.webhook_url = webhook_url


    def send_message(self, message):

        payload = {"text": message}

        try:

            response = requests.post(
                self.webhook_url,
                json=payload
            )

            if response.status_code == 200:
                logger.info("Teams alert sent successfully")

            else:
                logger.error("Teams alert failed")

        except Exception as e:
            logger.error(str(e))


    def send_failure_alert(
        self,
        table,
        column,
        rule_type,
        failure_percentage,
        threshold
    ):

        message = f"""
🚨 Data Quality Failure

Table : {table}
Column : {column}
Rule : {rule_type}

Failure % : {round(failure_percentage,4)}
Threshold : {threshold}
"""

        self.send_message(message)


    def send_summary_alert(
        self,
        tables_checked,
        rules_executed,
        pass_count,
        fail_count
    ):

        message = f"""
📊 Data Quality Execution Summary

Tables Checked : {tables_checked}
Rules Executed : {rules_executed}

PASS : {pass_count}
FAIL : {fail_count}
"""

        self.send_message(message)