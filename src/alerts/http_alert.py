import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HTTPAlert:

    def __init__(self, flow_url):

        self.flow_url = flow_url

    def send_failure_alert(
        self,
        table,
        column,
        rule,
        failure_percentage,
        threshold
    ):

        if not self.flow_url:
            logger.warning("Power Automate URL not configured")
            return

        payload = {
            "table": table,
            "column": column,
            "rule": rule,
            "failure_percentage": float(failure_percentage),
            "threshold": float(threshold)
        }

        try:

            response = requests.post(
                self.flow_url,
                json=payload,
                timeout=10
            )

            if response.status_code in [200, 202]:
                logger.info("✅ HTTP alert sent successfully")

            else:
                logger.error(
                    f"❌ HTTP alert failed: {response.text}"
                )

        except Exception as e:

            logger.error(
                f"❌ HTTP alert error: {str(e)}"
            )