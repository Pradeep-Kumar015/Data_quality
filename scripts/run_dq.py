from src.core.dq_runner import DQRunner
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():

    try:

        logger.info("Starting DQ Framework execution")

        runner = DQRunner()

        # Execute framework
        tables_checked, rules_executed, pass_count, fail_count = runner.run()

        # Log summary
        logger.info("DQ Execution Summary:")
        logger.info(f"Tables Checked: {tables_checked}")
        logger.info(f"Rules Executed: {rules_executed}")
        logger.info(f"Passed: {pass_count}")
        logger.info(f"Failed: {fail_count}")

        # CLI summary output
        print("\n===== DQ EXECUTION SUMMARY =====")
        print(f"Tables Checked   : {tables_checked}")
        print(f"Rules Executed   : {rules_executed}")
        print(f"Passed           : {pass_count}")
        print(f"Failed           : {fail_count}")
        print("================================\n")

        # 🚨 Critical for Airflow orchestration
        # If any rule fails → mark DAG task FAILED
        if fail_count > 0:

            logger.error(
                "DQ validation failed. One or more rules breached thresholds."
            )

            raise RuntimeError(
                "Data Quality validation failed"
            )

        logger.info("DQ Framework execution completed successfully ✅")

    except Exception as e:

        logger.error(f"DQ execution failed: {str(e)}")

        # Ensures Airflow marks task as FAILED
        raise


if __name__ == "__main__":
    main()