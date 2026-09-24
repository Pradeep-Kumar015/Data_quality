import os

from src.core.dq_runner import DQRunner
from src.utils.logger import get_logger


logger = get_logger(__name__)


def main():

    try:

        logger.info("Starting DQ Framework execution")

        runner = DQRunner()

        # --------------------------------------------------
        # Execute framework
        # Returns 5 metrics
        # --------------------------------------------------
        (
            tables_checked,
            rules_executed,
            pass_count,
            fail_count,
            critical_fail_count
        ) = runner.run()

        # --------------------------------------------------
        # DQ Execution Summary
        # --------------------------------------------------
        logger.info("")
        logger.info("===== DQ EXECUTION SUMMARY =====")
        logger.info("Tables Checked   : %s", tables_checked)
        logger.info("Rules Executed   : %s", rules_executed)
        logger.info("Passed           : %s", pass_count)
        logger.info("Failed           : %s", fail_count)
        logger.info("Critical Failed  : %s", critical_fail_count)
        logger.info("================================")
        logger.info("")

        # --------------------------------------------------
        # Environment-aware pipeline stop logic
        # Only stop pipeline in PROD
        # --------------------------------------------------
        environment = os.getenv(
            "ENVIRONMENT",
            "DEV"
        ).upper()

        logger.info(
            "DQ Environment: %s",
            environment
        )

        if critical_fail_count > 0 and environment == "PROD":

            logger.error(
                "Critical Data Quality rules failed. "
                "Stopping pipeline execution (PROD mode)."
            )

            raise RuntimeError(
                "Critical Data Quality validation failed"
            )

        elif critical_fail_count > 0:

            logger.warning(
                "Critical rules failed but pipeline continues "
                "(%s mode).",
                environment
            )

        else:

            logger.info(
                "DQ Framework execution completed successfully"
            )

    except Exception as e:

        logger.error(
            "DQ execution failed: %s",
            str(e)
        )

        # Required so Airflow marks the task as FAILED
        raise


if __name__ == "__main__":
    main()