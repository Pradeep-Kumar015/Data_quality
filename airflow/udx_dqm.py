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

SUBJECT_AREA: str = "udx_maximo_5_load"

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
# DQM EXECUTION
# ============================================================

def download_and_run_dqm():
    """
    Download DQ.zip using the Organization S3 configuration,
    extract the DQM project, install DQM dependencies into a
    temporary directory, and execute scripts/run_dq.py.
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

    # IMPORTANT:
    # Use the existing Organization S3 configuration.
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

            logger.info(
                "Files found inside DQ.zip:"
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
    #
    # Packages are installed into:
    #
    # /tmp/udx_maximo_dqm/python_packages
    #
    # We do NOT modify the shared Airflow Python environment.
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

    logger.info(
        "Running pip installation..."
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

    if install_output.strip():

        logger.info(
            "%s",
            install_output,
        )

    else:

        logger.info(
            "<NO PIP OUTPUT>"
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
    # 9. Verify python-dotenv
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

    # Create environment for dependency verification.

    env = os.environ.copy()

    env["PYTHONUNBUFFERED"] = "1"

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

    logger.info(
        "python-dotenv is available to the DQM process."
    )

    # --------------------------------------------------------
    # 10. Check common DQM directories
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
    # 11. Validate Python environment
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
    # 12. Verify Python module discovery
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
    # 13. Execute DQM
    #
    # Local command:
    #
    # python3 -m scripts.run_dq
    #
    # We intentionally use module mode because the DQM
    # project imports modules from src.
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

    # --------------------------------------------------------
    # 14. Run DQM
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
    # 15. Capture DQM output
    # --------------------------------------------------------

    dqm_output = result.stdout or ""

    logger.info("==================================================")
    logger.info("DQM PROCESS OUTPUT START")
    logger.info("==================================================")

    if dqm_output.strip():

        logger.info(
            "%s",
            dqm_output,
        )

    else:

        logger.info(
            "<DQM PROCESS PRODUCED NO STDOUT/STDERR OUTPUT>"
        )

    logger.info("==================================================")
    logger.info("DQM PROCESS OUTPUT END")
    logger.info("==================================================")

    logger.info(
        "DQM return code: %s",
        result.returncode,
    )

    # --------------------------------------------------------
    # 16. Validate DQM execution
    # --------------------------------------------------------

    if result.returncode != 0:

        logger.error(
            "DQM execution failed."
        )

        logger.error(
            "DQM return code: %s",
            result.returncode,
        )

        if dqm_output.strip():

            error_details = dqm_output.strip()

        else:

            error_details = (
                "DQM process returned a non-zero exit code "
                "but produced no stdout/stderr output."
            )

        raise RuntimeError(
            "DQM execution failed.\n\n"
            f"Return code: {result.returncode}\n\n"
            "DQM output:\n"
            f"{error_details}"
        )

    # --------------------------------------------------------
    # 17. DQM successful
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