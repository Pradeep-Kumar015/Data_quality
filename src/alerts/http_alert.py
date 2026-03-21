import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HTTPAlert:

    def __init__(self, url):
        self.url = url

    def send_failure_alert(
        self,
        table,
        column,
        rule,
        failure_percentage,
        threshold
    ):

        payload = {
            "table": table,
            "column": column,
            "rule": rule,
            "failure_percentage": failure_percentage,
            "threshold": threshold
        }

        try:
            response = requests.post(self.url, json=payload)

            if response.status_code in [200, 202]:
                logger.info("✅ HTTP alert sent successfully")
            else:
                logger.error(f"❌ HTTP alert failed: {response.text}")

        except Exception as e:
            logger.error(f"❌ HTTP alert error: {str(e)}")