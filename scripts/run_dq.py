from src.core.dq_runner import DQRunner
from src.utils.logger import get_logger
import os


logger = get_logger(__name__)


def main():

    try:

        logger.info("Starting DQ Framework execution")

        runner = DQRunner()

        # --------------------------------------------------
        # Execute framework
        # Returns 5 metrics now
        # --------------------------------------------------
        (
            tables_checked,
            rules_executed,
            pass_count,
            fail_count,
            critical_fail_count
        ) = runner.run()

        # --------------------------------------------------
        # Log execution summary
        # --------------------------------------------------
        logger.info("DQ Execution Summary:")
        logger.info(f"Tables Checked: {tables_checked}")
        logger.info(f"Rules Executed: {rules_executed}")
        logger.info(f"Passed: {pass_count}")
        logger.info(f"Failed: {fail_count}")
        logger.info(f"Critical Failed: {critical_fail_count}")

        # --------------------------------------------------
        # CLI-friendly output
        # --------------------------------------------------
        print("\n===== DQ EXECUTION SUMMARY =====")
        print(f"Tables Checked   : {tables_checked}")
        print(f"Rules Executed   : {rules_executed}")
        print(f"Passed           : {pass_count}")
        print(f"Failed           : {fail_count}")
        print(f"Critical Failed  : {critical_fail_count}")
        print("================================\n")

        # --------------------------------------------------
        # Environment-aware pipeline stop logic
        # Only stop in PROD environment
        # --------------------------------------------------
        environment = os.getenv("ENVIRONMENT", "DEV")

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
                "(DEV mode)."
            )

        else:

            logger.info(
                "DQ Framework execution completed successfully ✅"
            )

    except Exception as e:

        logger.error(f"DQ execution failed: {str(e)}")

        # Required so Airflow marks task FAILED
        raise


if __name__ == "__main__":
    main()