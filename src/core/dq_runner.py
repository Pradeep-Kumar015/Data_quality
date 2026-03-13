from connectors.snowflake_connector import SnowflakeConnector
from config_loader.db_config_loader import DBConfigLoader
from core.dq_engine import DQEngine

from alerts.teams_alert import TeamsAlert
from alerts.email_alert import EmailAlert

from utils.logger import get_logger

from dotenv import load_dotenv
import os

# ---------------- LOAD ENV VARIABLES ----------------
load_dotenv()

logger = get_logger(__name__)

# ---------------- ALERT CONFIG ----------------
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK_URL")

EMAIL_SMTP = os.getenv("EMAIL_SMTP")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",")


class DQRunner:

    def run(self):

        logger.info("Starting Data Quality Framework")

        # ---------------- SNOWFLAKE SESSION ----------------
        try:
            session = SnowflakeConnector().create_session()
            logger.info("Snowflake session created successfully")

        except Exception as e:
            logger.error(f"Snowflake connection failed: {str(e)}")
            raise

        # ---------------- LOAD CONFIG ----------------
        try:
            config_loader = DBConfigLoader(session)

            dq_config_df = config_loader.load_active_rules()

            rule_lookup = config_loader.load_rule_lookup()

            logger.info("DQ configuration loaded")

        except Exception as e:
            logger.error(f"Failed to load DQ configuration: {str(e)}")
            raise

        # ---------------- EXECUTE ENGINE ----------------
        engine = DQEngine(
            session,
            rule_lookup,
            TEAMS_WEBHOOK
        )

        tables_checked, rules_executed, pass_count, fail_count = \
            engine.execute(dq_config_df)

        logger.info("DQ execution finished")

        # ---------------- TEAMS SUMMARY ALERT ----------------
        try:
            teams_alert = TeamsAlert(TEAMS_WEBHOOK)

            teams_alert.send_summary_alert(
                tables_checked,
                rules_executed,
                pass_count,
                fail_count
            )

            logger.info("Teams summary alert sent")

        except Exception as e:
            logger.error(f"Teams alert failed: {str(e)}")

        # ---------------- EMAIL FAILURE ALERT ----------------
        if fail_count > 0:

            try:
                email_alert = EmailAlert(
                    EMAIL_SMTP,
                    EMAIL_PORT,
                    EMAIL_USER,
                    EMAIL_PASSWORD,
                    EMAIL_RECIPIENTS
                )

                email_alert.send_failure_summary(
                    rules_executed,
                    pass_count,
                    fail_count
                )

                logger.info("Failure summary email sent")

            except Exception as e:
                logger.error(f"Email alert failed: {str(e)}")