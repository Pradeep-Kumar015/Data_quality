from connectors.snowflake_connector import SnowflakeConnector
from config_loader.db_config_loader import DBConfigLoader
from core.dq_engine import DQEngine

from alerts.teams_alert import TeamsAlert
from alerts.email_alert import EmailAlert

from utils.logger import get_logger

logger = get_logger(__name__)


# ================= ALERT CONFIG =================

TEAMS_WEBHOOK = "YOUR_TEAMS_WEBHOOK"

EMAIL_SMTP = "smtp.office365.com"
EMAIL_PORT = 587
EMAIL_USER = "dqframework@company.com"
EMAIL_PASSWORD = "PASSWORD"

EMAIL_RECIPIENTS = ["data-team@company.com"]


class DQRunner:

    def run(self):

        logger.info("Starting Data Quality Framework")

        # ---------------- SNOWFLAKE SESSION ----------------
        session = SnowflakeConnector().create_session()

        # ---------------- LOAD CONFIG ----------------
        config_loader = DBConfigLoader(session)

        dq_config_df = config_loader.load_active_rules()

        rule_lookup = config_loader.load_rule_lookup()

        # ---------------- EXECUTE ENGINE ----------------
        engine = DQEngine(
            session,
            rule_lookup,
            TEAMS_WEBHOOK
        )

        tables_checked, rules_executed, pass_count, fail_count = \
            engine.execute(dq_config_df)

        logger.info("DQ execution finished")

        # ---------------- ALERTS ----------------

        teams_alert = TeamsAlert(TEAMS_WEBHOOK)

        teams_alert.send_summary_alert(
            tables_checked,
            rules_executed,
            pass_count,
            fail_count
        )

        # Send Outlook email only if failures exist
        if fail_count > 0:

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