from src.connectors.snowflake_connector import SnowflakeConnector
from src.config_loader.db_config_loader import DBConfigLoader
from src.core.dq_engine import DQEngine
from src.alerts.teams_alert import TeamsAlert
from src.utils.logger import get_logger

from dotenv import load_dotenv
import os


# --------------------------------------------------
# LOAD ENV VARIABLES
# --------------------------------------------------
load_dotenv()

logger = get_logger(__name__)


# --------------------------------------------------
# ALERT CONFIG
# --------------------------------------------------
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK_URL")


class DQRunner:

    def run(self):

        logger.info("Starting Data Quality Framework execution")

        # --------------------------------------------------
        # STEP 1: CREATE SNOWFLAKE SESSION
        # --------------------------------------------------
        try:

            session = SnowflakeConnector().create_session()

            logger.info(
                "Snowflake session created successfully"
            )

        except Exception as e:

            logger.error(
                f"Snowflake connection failed: {str(e)}"
            )

            raise


        # --------------------------------------------------
        # STEP 2: LOAD CONFIGURATION
        # --------------------------------------------------
        try:

            config_loader = DBConfigLoader(session)

            dq_config_df = config_loader.load_active_rules()

            if dq_config_df.count() == 0:

                logger.warning(
                    "No active DQ rules found. Skipping execution."
                )

                return 0, 0, 0, 0, 0

            rule_lookup = config_loader.load_rule_lookup()

            logger.info(
                "DQ configuration loaded successfully"
            )

        except Exception as e:

            logger.error(
                f"Failed to load configuration: {str(e)}"
            )

            raise


        # --------------------------------------------------
        # STEP 3: EXECUTE DQ ENGINE
        # --------------------------------------------------
        try:

            (
                tables_checked,
                rules_executed,
                pass_count,
                fail_count,
                critical_fail_count
            ) = DQEngine(
                session=session,
                rule_lookup=rule_lookup,
                teams_webhook=TEAMS_WEBHOOK
            ).execute(dq_config_df)

            logger.info(
                "DQ execution completed successfully"
            )

        except Exception as e:

            logger.error(
                f"DQ execution failed: {str(e)}"
            )

            raise


        # --------------------------------------------------
        # STEP 4: SEND SUMMARY ALERT (OPTIONAL)
        # --------------------------------------------------
        if TEAMS_WEBHOOK and rules_executed > 0:

            try:

                TeamsAlert(
                    TEAMS_WEBHOOK
                ).send_summary_alert(
                    tables_checked,
                    rules_executed,
                    pass_count,
                    fail_count
                )

                logger.info(
                    "Teams summary alert sent successfully 📊"
                )

            except Exception as e:

                logger.error(
                    f"Teams summary alert failed: {str(e)}"
                )

        else:

            logger.info(
                "Skipping Teams summary alert "
                "(webhook missing or no rules executed)"
            )


        # --------------------------------------------------
        # STEP 5: FINAL EXECUTION SUMMARY
        # --------------------------------------------------
        logger.info(
            "DQ Execution Summary | "
            f"Tables: {tables_checked}, "
            f"Rules: {rules_executed}, "
            f"Pass: {pass_count}, "
            f"Fail: {fail_count}, "
            f"Critical Fail: {critical_fail_count}"
        )


        # --------------------------------------------------
        # STEP 6: RETURN EXECUTION METRICS
        # --------------------------------------------------
        return (
            tables_checked,
            rules_executed,
            pass_count,
            fail_count,
            critical_fail_count
        )