import io
import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile

from airflow import DAG
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from lib import util
from lib.interfaces.airflow import config_loader, task
from lib.interfaces.constants import Config
import lib.interfaces.constants as constants

from lib.s3_util import S3Services


print("constants.py location:", constants.__file__)

logger = logging.getLogger(__name__)


# ============================================================
# DQM CONFIGURATION
# ============================================================

SUBJECT_AREA: str = "udx_maximo_dqm"

DAG_ID: str = "udx_maximo_dqm"

S3_BUCKET: str = "udx-entdata-pp-airflow"

S3_KEY: str = "DQ.zip"

WORK_DIR: str = "/tmp/udx_maximo_dqm"

EXTRACT_DIR: str = os.path.join(
    WORK_DIR,
    "dqm",
)

PYTHON_PACKAGES_DIR: str = os.path.join(
    WORK_DIR,
    "python_packages",
)


# ============================================================
# EXISTING AIRFLOW SNOWFLAKE CONNECTION
# ============================================================

SNOWFLAKE_CONNECTION_ID: str = "snowflake_maximo_conn"


# ============================================================
# DQM EXECUTION
# ============================================================

def download_and_run_dqm():
    """
    Download DQ.zip using the existing Organization S3
    configuration, extract the DQM project, install DQM
    dependencies into a temporary directory, configure the
    DQM process to use the existing Airflow Snowflake
    connection, and execute scripts/run_dq.py.

    Existing Airflow Snowflake connection:

        snowflake_maximo_conn

    Snowflake source database:

        UDX_CORE_UAT

    DQM metadata database:

        BI_DATA_QUALITY_UAT

    The DQM metadata tables must use fully-qualified
    database.schema.table names.

    No local Snowflake JSON connection is used.

    No second Snowflake connection is required.

    The Snowflake private-key passphrase is retrieved by
    SnowflakeConnector directly from the Password field of
    the existing Airflow connection.
    """

    logger.info("==================================================")
    logger.info("Starting DQM execution")
    logger.info("==================================================")

    # --------------------------------------------------------
    # 1. Clean previous DQM execution directory
    # --------------------------------------------------------

    if os.path.exists(WORK_DIR):

        logger.info(
            "Removing previous DQM work directory: %s",
            WORK_DIR,
        )

        shutil.rmtree(WORK_DIR)

    os.makedirs(
        EXTRACT_DIR,
        exist_ok=True,
    )

    os.makedirs(
        PYTHON_PACKAGES_DIR,
        exist_ok=True,
    )

    logger.info(
        "DQM work directory: %s",
        WORK_DIR,
    )

    logger.info(
        "DQM extraction directory: %s",
        EXTRACT_DIR,
    )

    logger.info(
        "DQM Python packages directory: %s",
        PYTHON_PACKAGES_DIR,
    )

    # --------------------------------------------------------
    # 2. Initialize Organization S3 service
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Initializing Organization S3 service")
    logger.info("==================================================")

    logger.info(
        "S3 bucket: %s",
        S3_BUCKET,
    )

    logger.info(
        "S3 key: %s",
        S3_KEY,
    )

    logger.info(
        "Subject area: %s",
        SUBJECT_AREA,
    )

    # Use existing Organization S3 configuration.
    #
    # No new S3Hook.
    # No new AWS connection.
    # No new AWS credentials.

    s3_service = S3Services(
        bucket=S3_BUCKET,
        feed_name=SUBJECT_AREA,
    )

    logger.info(
        "Organization S3 service initialized successfully."
    )

    # --------------------------------------------------------
    # 3. Download DQ.zip
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Downloading DQ.zip")
    logger.info("==================================================")

    logger.info(
        "Downloading s3://%s/%s",
        S3_BUCKET,
        S3_KEY,
    )

    try:

        response = s3_service.s3_hook.get_conn().get_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
        )

        zip_content = response["Body"].read()

    except Exception as exc:

        logger.error(
            "Failed to download DQ.zip from S3.",
            exc_info=True,
        )

        raise RuntimeError(
            f"Failed to download s3://{S3_BUCKET}/{S3_KEY}"
        ) from exc

    logger.info(
        "DQ.zip downloaded successfully."
    )

    logger.info(
        "DQ.zip size: %s bytes",
        len(zip_content),
    )

    if not zip_content:

        raise RuntimeError(
            "DQ.zip downloaded from S3 is empty."
        )

    # --------------------------------------------------------
    # 4. Extract DQ.zip
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Extracting DQ.zip")
    logger.info("==================================================")

    logger.info(
        "Extraction directory: %s",
        EXTRACT_DIR,
    )

    try:

        with zipfile.ZipFile(
            io.BytesIO(zip_content),
            "r",
        ) as zip_ref:

            zip_files = zip_ref.namelist()

            logger.info(
                "Number of files inside DQ.zip: %s",
                len(zip_files),
            )

            for file_name in zip_files:

                logger.info(
                    "  %s",
                    file_name,
                )

            zip_ref.extractall(
                EXTRACT_DIR
            )

    except zipfile.BadZipFile as exc:

        logger.error(
            "DQ.zip is not a valid ZIP file.",
            exc_info=True,
        )

        raise RuntimeError(
            "DQ.zip downloaded from S3 is not a valid ZIP file."
        ) from exc

    logger.info(
        "DQ.zip extracted successfully."
    )

    # --------------------------------------------------------
    # 5. Locate scripts/run_dq.py
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Searching for scripts/run_dq.py")
    logger.info("==================================================")

    run_dq_file = None

    for root, _, files in os.walk(EXTRACT_DIR):

        if (
            "run_dq.py" in files
            and os.path.basename(root) == "scripts"
        ):

            run_dq_file = os.path.join(
                root,
                "run_dq.py",
            )

            break

    if not run_dq_file:

        raise FileNotFoundError(
            "Could not find scripts/run_dq.py inside DQ.zip"
        )

    logger.info(
        "DQM entry point found: %s",
        run_dq_file,
    )

    # --------------------------------------------------------
    # 6. Determine DQM project directory
    # --------------------------------------------------------

    project_dir = os.path.dirname(
        os.path.dirname(
            run_dq_file
        )
    )

    logger.info(
        "DQM project directory: %s",
        project_dir,
    )

    if not os.path.isdir(project_dir):

        raise FileNotFoundError(
            f"DQM project directory does not exist: "
            f"{project_dir}"
        )

    scripts_dir = os.path.join(
        project_dir,
        "scripts",
    )

    if not os.path.isdir(scripts_dir):

        raise FileNotFoundError(
            f"DQM scripts directory does not exist: "
            f"{scripts_dir}"
        )

    if not os.path.isfile(run_dq_file):

        raise FileNotFoundError(
            f"DQM entry point does not exist: "
            f"{run_dq_file}"
        )

    logger.info(
        "DQM entry point verified."
    )

    # --------------------------------------------------------
    # 7. Locate requirements.txt
    # --------------------------------------------------------

    requirements_file = os.path.join(
        project_dir,
        "requirements.txt",
    )

    logger.info(
        "DQM requirements file: %s",
        requirements_file,
    )

    if not os.path.isfile(requirements_file):

        raise FileNotFoundError(
            "requirements.txt was not found inside DQ.zip. "
            f"Expected location: {requirements_file}"
        )

    logger.info(
        "requirements.txt found successfully."
    )

    # --------------------------------------------------------
    # 8. Install DQM dependencies
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Installing DQM Python dependencies")
    logger.info("==================================================")

    install_command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--target",
        PYTHON_PACKAGES_DIR,
        "-r",
        requirements_file,
        "--disable-pip-version-check",
    ]

    logger.info(
        "Installing dependencies from: %s",
        requirements_file,
    )

    logger.info(
        "Python executable used for installation: %s",
        sys.executable,
    )

    logger.info(
        "Package installation directory: %s",
        PYTHON_PACKAGES_DIR,
    )

    try:

        install_result = subprocess.run(
            install_command,
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    except Exception as exc:

        logger.error(
            "Unable to start pip installation.",
            exc_info=True,
        )

        raise RuntimeError(
            "Unable to install DQM Python dependencies."
        ) from exc

    install_output = install_result.stdout or ""

    logger.info("==================================================")
    logger.info("PIP INSTALL OUTPUT")
    logger.info("==================================================")

    logger.info(
        "%s",
        install_output or "<NO PIP OUTPUT>",
    )

    logger.info("==================================================")

    logger.info(
        "pip install return code: %s",
        install_result.returncode,
    )

    if install_result.returncode != 0:

        raise RuntimeError(
            "Failed to install DQM Python dependencies.\n\n"
            "Pip output:\n"
            f"{install_output or '<NO OUTPUT>'}"
        )

    logger.info(
        "DQM Python dependencies installed successfully."
    )

    # --------------------------------------------------------
    # 9. Build DQM subprocess environment
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Building DQM execution environment")
    logger.info("==================================================")

    env = os.environ.copy()

    env["PYTHONUNBUFFERED"] = "1"

    # --------------------------------------------------------
    # 9A. AIRFLOW SNOWFLAKE CONNECTION
    # --------------------------------------------------------
    #
    # The DQM SnowflakeConnector reads this connection ID
    # and retrieves the complete connection from Airflow.
    #
    # The connector also reads the private-key passphrase
    # directly from the Password field of this connection.
    #
    # The passphrase is NOT copied into an environment
    # variable by this DAG.
    # --------------------------------------------------------

    env["SNOWFLAKE_AIRFLOW_CONNECTION_ID"] = (
        SNOWFLAKE_CONNECTION_ID
    )

    logger.info(
        "Airflow Snowflake connection ID: %s",
        SNOWFLAKE_CONNECTION_ID,
    )

    logger.info(
        "DQM Snowflake connector will retrieve the "
        "private-key passphrase directly from the "
        "Airflow connection Password field."
    )

    # --------------------------------------------------------
    # 10. Configure PYTHONPATH
    # --------------------------------------------------------

    existing_pythonpath = env.get(
        "PYTHONPATH",
        "",
    )

    python_paths = [
        PYTHON_PACKAGES_DIR,
        project_dir,
    ]

    if existing_pythonpath:

        python_paths.append(
            existing_pythonpath
        )

    env["PYTHONPATH"] = os.pathsep.join(
        python_paths
    )

    logger.info(
        "DQM PYTHONPATH: %s",
        env["PYTHONPATH"],
    )

    # --------------------------------------------------------
    # 11. Verify python-dotenv
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Verifying python-dotenv installation")
    logger.info("==================================================")

    dotenv_check_command = [
        sys.executable,
        "-u",
        "-c",
        (
            "import dotenv; "
            "print('python-dotenv imported successfully'); "
            "print('dotenv location:', dotenv.__file__)"
        ),
    ]

    try:

        dotenv_check = subprocess.run(
            dotenv_check_command,
            cwd=project_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    except Exception as exc:

        logger.error(
            "Unable to verify python-dotenv.",
            exc_info=True,
        )

        raise RuntimeError(
            "Unable to verify python-dotenv installation."
        ) from exc

    logger.info(
        "python-dotenv verification return code: %s",
        dotenv_check.returncode,
    )

    logger.info(
        "python-dotenv verification output:\n%s",
        dotenv_check.stdout or "<NO OUTPUT>",
    )

    if dotenv_check.returncode != 0:

        raise RuntimeError(
            "python-dotenv installation verification failed.\n\n"
            "Verification output:\n"
            f"{dotenv_check.stdout or '<NO OUTPUT>'}"
        )

    # --------------------------------------------------------
    # 12. Check DQM directories
    # --------------------------------------------------------

    src_dir = os.path.join(
        project_dir,
        "src",
    )

    connection_dir = os.path.join(
        project_dir,
        "connection",
    )

    logger.info("==================================================")
    logger.info("DQM project validation")
    logger.info("==================================================")

    logger.info(
        "src directory exists: %s",
        os.path.isdir(src_dir),
    )

    logger.info(
        "connection directory exists: %s",
        os.path.isdir(connection_dir),
    )

    logger.info(
        "scripts directory exists: %s",
        os.path.isdir(scripts_dir),
    )

    logger.info(
        "run_dq.py exists: %s",
        os.path.isfile(run_dq_file),
    )

    # --------------------------------------------------------
    # 13. Validate Python environment
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("DQM Python environment")
    logger.info("==================================================")

    logger.info(
        "Python executable: %s",
        sys.executable,
    )

    logger.info(
        "Python version: %s",
        sys.version,
    )

    logger.info(
        "Current Airflow process directory: %s",
        os.getcwd(),
    )

    logger.info(
        "DQM project directory: %s",
        project_dir,
    )

    logger.info(
        "DQM PYTHONPATH: %s",
        env["PYTHONPATH"],
    )

    # --------------------------------------------------------
    # 14. Verify Python module discovery
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("Checking DQM Python module discovery")
    logger.info("==================================================")

    module_check_command = [
        sys.executable,
        "-u",
        "-c",
        (
            "import sys; "
            "import importlib.util; "
            "print('PYTHON_EXECUTABLE=' + sys.executable); "
            "print('PYTHON_VERSION=' + sys.version); "
            "print('PYTHON_PATH=' + repr(sys.path)); "
            "print('SCRIPTS_SPEC=' + repr(importlib.util.find_spec('scripts'))); "
            "print('SRC_SPEC=' + repr(importlib.util.find_spec('src'))); "
            "import dotenv; "
            "print('DOTENV_SPEC=' + repr(dotenv.__file__))"
        ),
    ]

    try:

        module_check = subprocess.run(
            module_check_command,
            cwd=project_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    except Exception as exc:

        logger.error(
            "Unable to perform Python module discovery check.",
            exc_info=True,
        )

        raise RuntimeError(
            "DQM Python module discovery check failed."
        ) from exc

    logger.info(
        "Python module discovery return code: %s",
        module_check.returncode,
    )

    logger.info(
        "Python module discovery output:\n%s",
        module_check.stdout or "<NO OUTPUT>",
    )

    if module_check.returncode != 0:

        raise RuntimeError(
            "DQM Python module discovery failed.\n\n"
            "Output:\n"
            f"{module_check.stdout or '<NO OUTPUT>'}"
        )

    # --------------------------------------------------------
    # 15. Execute DQM
    # --------------------------------------------------------

    command = [
        sys.executable,
        "-u",
        "-m",
        "scripts.run_dq",
    ]

    logger.info("==================================================")
    logger.info("Starting DQM process")
    logger.info("==================================================")

    logger.info(
        "Command: %s",
        " ".join(command),
    )

    logger.info(
        "Working directory: %s",
        project_dir,
    )

    logger.info(
        "Python executable: %s",
        sys.executable,
    )

    logger.info(
        "Airflow Snowflake connection ID: %s",
        env.get(
            "SNOWFLAKE_AIRFLOW_CONNECTION_ID"
        ),
    )

    logger.info(
        "Snowflake private-key passphrase will be "
        "retrieved directly by SnowflakeConnector "
        "from the Airflow connection."
    )

    # --------------------------------------------------------
    # 16. Run DQM
    # --------------------------------------------------------

    try:

        result = subprocess.run(
            command,
            cwd=project_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    except Exception as exc:

        logger.error(
            "Unable to start DQM subprocess.",
            exc_info=True,
        )

        raise RuntimeError(
            "Unable to start DQM subprocess."
        ) from exc

    # --------------------------------------------------------
    # 17. Capture DQM output
    # --------------------------------------------------------

    dqm_output = result.stdout or ""

    logger.info("==================================================")
    logger.info("DQM PROCESS OUTPUT START")
    logger.info("==================================================")

    logger.info(
        "%s",
        dqm_output or "<DQM PROCESS PRODUCED NO OUTPUT>",
    )

    logger.info("==================================================")
    logger.info("DQM PROCESS OUTPUT END")
    logger.info("==================================================")

    logger.info(
        "DQM return code: %s",
        result.returncode,
    )

    # --------------------------------------------------------
    # 18. Validate DQM execution
    # --------------------------------------------------------

    if result.returncode != 0:

        logger.error(
            "DQM execution failed."
        )

        error_details = (
            dqm_output.strip()
            if dqm_output.strip()
            else
            "DQM process returned a non-zero exit code "
            "but produced no output."
        )

        raise RuntimeError(
            "DQM execution failed.\n\n"
            f"Return code: {result.returncode}\n\n"
            "DQM output:\n"
            f"{error_details}"
        )

    # --------------------------------------------------------
    # 19. DQM successful
    # --------------------------------------------------------

    logger.info("==================================================")
    logger.info("DQM execution completed successfully.")
    logger.info("==================================================")


# ============================================================
# AIRFLOW DAG
# ============================================================

@dag(
    dag_id=DAG_ID,
    description="UDX Maximo Data Quality Framework",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=[
        "UDX",
        "DQM",
    ],
)
def generate_dynamic_dag():

    try:

        run_dqm = PythonOperator(
            task_id="run_dqm",
            python_callable=download_and_run_dqm,
        )

        return run_dqm

    except Exception as ce:

        logger.error(
            "Error while creating DQM DAG: %s",
            ce,
            exc_info=True,
        )

        raise


# ============================================================
# REGISTER DAG
# ============================================================

globals()[DAG_ID] = generate_dynamic_dag()