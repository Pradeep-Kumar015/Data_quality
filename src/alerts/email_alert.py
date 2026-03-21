import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from src.utils.logger import get_logger

logger = get_logger(__name__)


class EmailAlert:

    def __init__(
        self,
        smtp_server,
        smtp_port,
        sender_email,
        password,
        recipients
    ):

        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.password = password
        self.recipients = recipients


    def send_failure_summary(
        self,
        rules_executed,
        pass_count,
        fail_count
    ):

        subject = "Data Quality Failure Summary"

        body = f"""
Data Quality Execution Completed

Total Rules Executed : {rules_executed}
Passed               : {pass_count}
Failed               : {fail_count}

Please review the DQ_RESULT_TABLE for details.
"""

        message = MIMEMultipart()

        message["From"] = self.sender_email
        message["To"] = ", ".join(self.recipients)
        message["Subject"] = subject

        message.attach(MIMEText(body, "plain"))

        try:

            server = smtplib.SMTP(
                self.smtp_server,
                self.smtp_port
            )

            server.starttls()

            server.login(
                self.sender_email,
                self.password
            )

            server.sendmail(
                self.sender_email,
                self.recipients,
                message.as_string()
            )

            server.quit()

            logger.info("Email alert sent successfully")

        except Exception as e:

            logger.error(f"Email alert failed: {str(e)}")