from src.core.dq_runner import DQRunner
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():

    try:

        logger.info("Starting DQ Framework execution")

        runner = DQRunner()

        runner.run()

        logger.info("DQ Framework execution completed")

    except Exception as e:

        logger.error(f"DQ execution failed: {str(e)}")

        # Important for Airflow to mark task as FAILED
        raise


if __name__ == "__main__":

    main()