from src.core.dq_runner import DQRunner
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():

    try:
        logger.info("Starting DQ Framework execution")

        runner = DQRunner()

        # ✅ Capture return values
        tables_checked, rules_executed, pass_count, fail_count = runner.run()

        # ✅ Log final summary
        logger.info("DQ Execution Summary:")
        logger.info(f"Tables Checked: {tables_checked}")
        logger.info(f"Rules Executed: {rules_executed}")
        logger.info(f"Passed: {pass_count}")
        logger.info(f"Failed: {fail_count}")

        # ✅ Optional: print for CLI visibility
        print("\n===== DQ EXECUTION SUMMARY =====")
        print(f"Tables Checked   : {tables_checked}")
        print(f"Rules Executed   : {rules_executed}")
        print(f"Passed           : {pass_count}")
        print(f"Failed           : {fail_count}")
        print("================================\n")

        logger.info("DQ Framework execution completed")

    except Exception as e:
        logger.error(f"DQ execution failed: {str(e)}")

        # ❗ Important for Airflow (marks task as FAILED)
        raise


if __name__ == "__main__":
    main()