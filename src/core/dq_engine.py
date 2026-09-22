from datetime import datetime
from collections import defaultdict

from snowflake.snowpark.functions import col, current_date

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DQEngine:

    def __init__(
        self,
        session,
        rule_lookup,
        teams_webhook=None
    ):
        self.session = session
        self.rule_lookup = rule_lookup

        from src.reporting.report_generator import ReportGenerator

        self.reporter = ReportGenerator(
            session,
            teams_webhook
        )

        # ========================================================
        # SNOWFLAKE EXECUTION LOG TABLE
        # ========================================================

        self.execution_log_table = (
            "BI_DATA_QUALITY_UAT.DQT.DQ_EXECUTION_LOG"
        )

        # ========================================================
        # SNOWFLAKE RESULT TABLE
        # ========================================================

        self.result_table = (
            "BI_DATA_QUALITY_UAT.DQT.DQ_RESULT"
        )

    # ============================================================
    # CONFIGURATION HELPERS
    # ============================================================

    @staticmethod
    def _normalize_row(row):
        """
        Convert Snowpark Row / dictionary into a normal dictionary.

        Supports both COLUMN_NAME and COLUMN_NAMES.
        """

        if hasattr(row, "as_dict"):
            row_dict = row.as_dict()

        elif isinstance(row, dict):
            row_dict = row.copy()

        else:
            try:
                row_dict = dict(row)
            except Exception as exc:
                raise ValueError(
                    f"Unable to convert configuration row "
                    f"to dictionary: {row}"
                ) from exc

        row_dict = {
            str(key).upper(): value
            for key, value in row_dict.items()
        }

        # --------------------------------------------------------
        # Backward compatibility:
        # COLUMN_NAMES -> COLUMN_NAME
        # --------------------------------------------------------

        if not row_dict.get("COLUMN_NAME"):

            column_names = row_dict.get(
                "COLUMN_NAMES"
            )

            if column_names:
                row_dict["COLUMN_NAME"] = column_names

        return row_dict

    @staticmethod
    def _get_string(value):
        """
        Convert configuration value into a clean string.
        """

        if value is None:
            return None

        value = str(value).strip()

        if not value:
            return None

        return value

    @staticmethod
    def _get_key_columns(value):
        """
        Convert KEY_COLUMNS into a list.

        Supports:

            ITEM_SET_ID,ITEM_ID

        or:

            ["ITEM_SET_ID", "ITEM_ID"]

        or:

            ("ITEM_SET_ID", "ITEM_ID")
        """

        if value is None:
            return None

        if isinstance(
            value,
            (list, tuple, set)
        ):

            columns = [
                str(column).strip()
                for column in value
                if column is not None
                and str(column).strip()
            ]

            return columns if columns else None

        value = str(value).strip()

        if not value:
            return None

        return [
            column.strip()
            for column in value.split(",")
            if column.strip()
        ]

    # ============================================================
    # EXECUTION LOG - CHECK
    # ============================================================

    def is_already_processed(
        self,
        database,
        schema,
        table,
        rule_id,
        config_id,
        column_name
    ):
        """
        Check whether this DQ configuration has already completed.

        DQ_EXECUTION_LOG.STATUS represents execution completion.

        STATUS = COMPLETED
            Means the DQ configuration completed successfully.

        PASS / FAIL
            Belongs to DQ_RESULT and represents the actual
            data-quality result.
        """

        column_value = column_name or ""

        try:

            query = f"""
                SELECT COUNT(*) AS CNT
                FROM {self.execution_log_table}
                WHERE DATABASE_NAME = ?
                  AND SCHEMA_NAME = ?
                  AND TABLE_NAME = ?
                  AND RULE_ID = ?
                  AND CONFIG_ID = ?
                  AND COALESCE(COLUMN_NAME, '') = ?
                  AND STATUS = 'COMPLETED'
            """

            result = self.session.sql(
                query,
                params=[
                    database,
                    schema,
                    table,
                    rule_id,
                    config_id,
                    column_value
                ]
            ).collect()

            count = int(
                result[0]["CNT"]
            )

            logger.info(
                "Execution log check completed: "
                f"{database}.{schema}.{table}, "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_value}, "
                f"COMPLETED_COUNT={count}"
            )

            return count > 0

        except Exception as exc:

            logger.warning(
                "Unable to check execution log for "
                f"{database}.{schema}.{table}, "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_value}: {exc}. "
                "Continuing execution."
            )

            return False

    # ============================================================
    # EXECUTION LOG - INSERT + VERIFY
    # ============================================================

    def log_execution(
        self,
        database,
        schema,
        table,
        rule_id,
        config_id,
        column_name
    ):
        """
        Insert a successfully completed DQ execution.

        STATUS is always COMPLETED.

        DQ_RESULT contains the actual DQ PASS / FAIL result.

        This method:
            1. Inserts the execution log.
            2. Immediately verifies the inserted record.
            3. Returns True only when the record exists.
        """

        column_value = column_name or ""

        try:

            # ----------------------------------------------------
            # INSERT EXECUTION LOG
            # ----------------------------------------------------

            insert_sql = f"""
                INSERT INTO {self.execution_log_table}
                (
                    DATABASE_NAME,
                    SCHEMA_NAME,
                    TABLE_NAME,
                    RULE_ID,
                    CONFIG_ID,
                    COLUMN_NAME,
                    RUN_DATE,
                    STATUS,
                    LAST_RUN_TIME
                )
                SELECT
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    CURRENT_DATE(),
                    'COMPLETED',
                    CURRENT_TIMESTAMP()
            """

            self.session.sql(
                insert_sql,
                params=[
                    database,
                    schema,
                    table,
                    rule_id,
                    config_id,
                    column_value
                ]
            ).collect()

            logger.info(
                "Execution log INSERT completed: "
                f"TARGET={self.execution_log_table}, "
                f"DATABASE_NAME={database}, "
                f"SCHEMA_NAME={schema}, "
                f"TABLE_NAME={table}, "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_value}"
            )

            # ----------------------------------------------------
            # VERIFY INSERT
            # ----------------------------------------------------

            verify_sql = f"""
                SELECT COUNT(*) AS CNT
                FROM {self.execution_log_table}
                WHERE DATABASE_NAME = ?
                  AND SCHEMA_NAME = ?
                  AND TABLE_NAME = ?
                  AND RULE_ID = ?
                  AND CONFIG_ID = ?
                  AND COALESCE(COLUMN_NAME, '') = ?
                  AND STATUS = 'COMPLETED'
            """

            verify_result = self.session.sql(
                verify_sql,
                params=[
                    database,
                    schema,
                    table,
                    rule_id,
                    config_id,
                    column_value
                ]
            ).collect()

            verify_count = int(
                verify_result[0]["CNT"]
            )

            if verify_count > 0:

                logger.info(
                    "Execution log verified successfully: "
                    f"RULE_ID={rule_id}, "
                    f"CONFIG_ID={config_id}, "
                    f"COLUMN={column_value}, "
                    "STATUS=COMPLETED"
                )

                return True

            logger.error(
                "Execution log INSERT completed but "
                "verification failed: "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_value}"
            )

            return False

        except Exception as exc:

            logger.exception(
                "Failed to insert execution log: "
                f"TARGET={self.execution_log_table}, "
                f"DATABASE_NAME={database}, "
                f"SCHEMA_NAME={schema}, "
                f"TABLE_NAME={table}, "
                f"RULE_ID={rule_id}, "
                f"CONFIG_ID={config_id}, "
                f"COLUMN={column_value}, "
                f"ERROR={exc}"
            )

            return False

    # ============================================================
    # RESULT CHECK
    # ============================================================

    def is_result_available(
        self,
        config_id,
        rule_id,
        column_name
    ):
        """
        Check whether DQ_RESULT was successfully created.

        This protects against the following situation:

            1. DQ calculation succeeds.
            2. DQ_RESULT is inserted.
            3. ReportGenerator fails during a later operation.

        If DQ_RESULT exists, the DQ execution itself completed
        and DQ_EXECUTION_LOG should be marked COMPLETED.
        """

        column_value = column_name or ""

        try:

            query = f"""
                SELECT COUNT(*) AS CNT
                FROM {self.result_table}
                WHERE CONFIG_ID = ?
                  AND RULE_ID = ?
                  AND COALESCE(COLUMN_NAME, '') = ?
            """

            result = self.session.sql(
                query,
                params=[
                    config_id,
                    rule_id,
                    column_value
                ]
            ).collect()

            count = int(
                result[0]["CNT"]
            )

            logger.info(
                "DQ_RESULT verification completed: "
                f"CONFIG_ID={config_id}, "
                f"RULE_ID={rule_id}, "
                f"COLUMN={column_value}, "
                f"COUNT={count}"
            )

            return count > 0

        except Exception as exc:

            logger.exception(
                "Unable to verify DQ_RESULT: "
                f"CONFIG_ID={config_id}, "
                f"RULE_ID={rule_id}, "
                f"COLUMN={column_value}, "
                f"ERROR={exc}"
            )

            return False

    # ============================================================
    # VALIDATE CONFIGURATION
    # ============================================================

    def _validate_rule_config(
        self,
        row_dict
    ):
        """
        Validate configuration based on rule type.

        DQ_002:
            Requires KEY_COLUMNS.

        DQ_005:
            Requires CUSTOM_SQL.

        Other rules:
            Require COLUMN_NAME.
        """

        rule_id = self._get_string(
            row_dict.get("RULE_ID")
        )

        config_id = self._get_string(
            row_dict.get("CONFIG_ID")
        )

        if not rule_id:

            raise ValueError(
                f"RULE_ID missing: {row_dict}"
            )

        if not config_id:

            raise ValueError(
                f"CONFIG_ID missing for "
                f"{rule_id}: {row_dict}"
            )

        # ========================================================
        # DQ_002
        # ========================================================

        if rule_id == "DQ_002":

            key_columns = self._get_key_columns(
                row_dict.get("KEY_COLUMNS")
            )

            if not key_columns:

                raise ValueError(
                    f"KEY_COLUMNS missing for "
                    f"{rule_id}: {row_dict}"
                )

            return

        # ========================================================
        # DQ_005
        # ========================================================

        if rule_id == "DQ_005":

            custom_sql = self._get_string(
                row_dict.get("CUSTOM_SQL")
            )

            if not custom_sql:

                raise ValueError(
                    f"CUSTOM_SQL missing for "
                    f"{rule_id}: {row_dict}"
                )

            return

        # ========================================================
        # STANDARD COLUMN RULE
        # ========================================================

        column_name = self._get_string(
            row_dict.get("COLUMN_NAME")
        )

        if not column_name:

            raise ValueError(
                f"COLUMN_NAME missing for "
                f"{rule_id}: {row_dict}"
            )

    # ============================================================
    # LOAD SOURCE TABLE
    # ============================================================

    def _load_source_table(
        self,
        database,
        schema,
        table,
        rule_rows
    ):
        """
        Load source Snowflake table.

        Applies PARTITION_COLUMN/current-date filtering when
        configured.
        """

        full_table_name = (
            f"{database}.{schema}.{table}"
        )

        logger.info(
            f"Processing table: {full_table_name}"
        )

        df = self.session.table(
            full_table_name
        )

        total_count = df.count()

        if total_count == 0:

            logger.warning(
                f"No data in {full_table_name}"
            )

            return None, 0

        # ========================================================
        # FIND PARTITION COLUMN
        # ========================================================

        partition_column = None

        for row in rule_rows:

            candidate = self._get_string(
                row.get("PARTITION_COLUMN")
            )

            if candidate:

                partition_column = candidate
                break

        # ========================================================
        # APPLY CURRENT DATE FILTER
        # ========================================================

        if partition_column:

            df_columns = {
                str(column).upper()
                for column in df.columns
            }

            if partition_column.upper() in df_columns:

                today_df = df.filter(
                    col(partition_column).cast("DATE")
                    == current_date()
                )

                today_count = today_df.count()

                if today_count > 0:

                    logger.info(
                        f"Current-date rows found for "
                        f"{full_table_name}: "
                        f"{today_count}. "
                        "Running checks on current-date data."
                    )

                    df = today_df
                    total_count = today_count

                else:

                    logger.info(
                        f"No current-date rows found for "
                        f"{full_table_name}. "
                        f"Running checks on full table "
                        f"({total_count} rows)."
                    )

            else:

                logger.warning(
                    f"{full_table_name} does not contain "
                    f"partition column "
                    f"{partition_column}. "
                    "Running checks on full table."
                )

        logger.info(
            f"Rows available for validation for "
            f"{full_table_name}: {total_count}"
        )

        return df, total_count

    # ============================================================
    # GET RULE COLUMN KEY
    # ============================================================

    def _get_column_key(
        self,
        row_dict,
        rule_id
    ):
        """
        Determine the column/key used for execution logging
        and reporting.
        """

        # ========================================================
        # DQ_005
        # ========================================================

        if rule_id == "DQ_005":

            config_id = self._get_string(
                row_dict.get("CONFIG_ID")
            )

            return f"CUSTOM_SQL_{config_id}"

        # ========================================================
        # DQ_002
        # ========================================================

        if rule_id == "DQ_002":

            key_columns = self._get_key_columns(
                row_dict.get("KEY_COLUMNS")
            )

            if not key_columns:

                raise ValueError(
                    f"KEY_COLUMNS missing for "
                    f"{rule_id}: {row_dict}"
                )

            return ",".join(
                key_columns
            )

        # ========================================================
        # STANDARD RULE
        # ========================================================

        column_name = self._get_string(
            row_dict.get("COLUMN_NAME")
        )

        if not column_name:

            raise ValueError(
                f"COLUMN_NAME missing for "
                f"{rule_id}: {row_dict}"
            )

        return column_name

    # ============================================================
    # EXECUTE
    # ============================================================

    def execute(
        self,
        dq_config_df
    ):

        tables_checked = 0
        rules_executed = 0
        pass_count = 0
        fail_count = 0
        critical_fail_count = 0

        processed_tables = {}

        grouped_rules = defaultdict(list)

        # ========================================================
        # STEP 1 - LOAD CONFIGURATION
        # ========================================================

        logger.info(
            "Preparing DQ configuration from Snowflake table"
        )

        if hasattr(
            dq_config_df,
            "collect"
        ):

            rows = dq_config_df.collect()

        else:

            rows = dq_config_df

        if not rows:

            logger.warning(
                "No active DQ configuration found"
            )

            return (
                tables_checked,
                rules_executed,
                pass_count,
                fail_count,
                critical_fail_count
            )

        # ========================================================
        # STEP 2 - NORMALIZE AND GROUP CONFIGURATION
        # ========================================================

        for row in rows:

            row_dict = self._normalize_row(
                row
            )

            database = self._get_string(
                row_dict.get("DATABASE_NAME")
            )

            schema = self._get_string(
                row_dict.get("SCHEMA_NAME")
            )

            table = self._get_string(
                row_dict.get("TABLE_NAME")
            )

            rule_id = self._get_string(
                row_dict.get("RULE_ID")
            )

            config_id = self._get_string(
                row_dict.get("CONFIG_ID")
            )

            # ----------------------------------------------------
            # REQUIRED FIELDS
            # ----------------------------------------------------

            if not database:

                raise ValueError(
                    f"DATABASE_NAME missing: {row_dict}"
                )

            if not schema:

                raise ValueError(
                    f"SCHEMA_NAME missing: {row_dict}"
                )

            if not table:

                raise ValueError(
                    f"TABLE_NAME missing: {row_dict}"
                )

            if not rule_id:

                raise ValueError(
                    f"RULE_ID missing: {row_dict}"
                )

            if not config_id:

                raise ValueError(
                    f"CONFIG_ID missing: {row_dict}"
                )

            # ----------------------------------------------------
            # NORMALIZE COLUMN_NAMES -> COLUMN_NAME
            # ----------------------------------------------------

            column_name = self._get_string(
                row_dict.get("COLUMN_NAME")
            )

            if column_name:

                row_dict["COLUMN_NAME"] = (
                    column_name
                )

            # ----------------------------------------------------
            # VALIDATE
            # ----------------------------------------------------

            self._validate_rule_config(
                row_dict
            )

            # ----------------------------------------------------
            # GROUP
            # ----------------------------------------------------

            key = (
                database,
                schema,
                table,
                rule_id,
                config_id
            )

            grouped_rules[key].append(
                row_dict
            )

        logger.info(
            f"DQ configuration loaded: "
            f"{len(grouped_rules)} rule groups"
        )

        # ========================================================
        # STEP 3 - EXECUTE GROUPED RULES
        # ========================================================

        for (
            database,
            schema,
            table,
            rule_id,
            config_id
        ), rule_rows in grouped_rules.items():

            full_table_name = (
                f"{database}.{schema}.{table}"
            )

            # ====================================================
            # LOAD SOURCE TABLE ONCE
            # ====================================================

            if full_table_name not in processed_tables:

                try:

                    df, total_count = (
                        self._load_source_table(
                            database,
                            schema,
                            table,
                            rule_rows
                        )
                    )

                except Exception as exc:

                    logger.exception(
                        f"Unable to load table "
                        f"{full_table_name}: {exc}"
                    )

                    continue

                if (
                    df is None
                    or total_count == 0
                ):

                    continue

                processed_tables[
                    full_table_name
                ] = (
                    df,
                    total_count
                )

                tables_checked += 1

            # ====================================================
            # GET TABLE DATA
            # ====================================================

            df, total_count = (
                processed_tables[
                    full_table_name
                ]
            )

            # ====================================================
            # RULE METADATA
            # ====================================================

            rule_metadata = (
                self.rule_lookup.get(
                    rule_id
                )
            )

            if not rule_metadata:

                logger.error(
                    f"Rule metadata missing for "
                    f"{rule_id}"
                )

                continue

            rule_func = (
                rule_metadata["func"]
            )

            rule_name = (
                rule_metadata["name"]
            )

            # ====================================================
            # EXECUTE CONFIGURATION ROWS
            # ====================================================

            for row_dict in rule_rows:

                column_name = self._get_string(
                    row_dict.get("COLUMN_NAME")
                )

                threshold = float(
                    row_dict.get("THRESHOLD")
                    or 0.0
                )

                severity = (
                    self._get_string(
                        row_dict.get("SEVERITY")
                    )
                    or "LOW"
                ).upper()

                min_val = row_dict.get(
                    "MIN_VALUE"
                )

                max_val = row_dict.get(
                    "MAX_VALUE"
                )

                # =================================================
                # COLUMN KEY
                # =================================================

                column_key = (
                    self._get_column_key(
                        row_dict,
                        rule_id
                    )
                )

                # =================================================
                # CHECK EXECUTION LOG
                # =================================================

                if self.is_already_processed(
                    database,
                    schema,
                    table,
                    rule_id,
                    config_id,
                    column_key
                ):

                    logger.info(
                        f"Skipping {rule_id} "
                        f"CONFIG_ID={config_id} "
                        f"COLUMN={column_key} "
                        "(execution already COMPLETED)"
                    )

                    continue

                # =================================================
                # START EXECUTION
                # =================================================

                start_time = datetime.now()

                query_id = None
                query_history = None
                failed_df = None
                failed_count = 0
                rule_expression = ""

                # =================================================
                # DQ RULE EXECUTION
                # =================================================

                try:

                    with self.session.query_history(
                        True
                    ) as qh:

                        query_history = qh

                        # =========================================
                        # DQ_005 - CUSTOM SQL
                        # =========================================

                        if rule_id == "DQ_005":

                            custom_sql = (
                                self._get_string(
                                    row_dict.get(
                                        "CUSTOM_SQL"
                                    )
                                )
                            )

                            if not custom_sql:

                                raise ValueError(
                                    f"CUSTOM_SQL missing for "
                                    f"{rule_id}"
                                )

                            (
                                failed_df,
                                failed_count,
                                rule_expression
                            ) = rule_func(
                                self.session,
                                custom_sql
                            )

                            column_name_for_report = (
                                column_key
                            )

                        # =========================================
                        # DQ_002 - UNIQUE CHECK
                        # =========================================

                        elif rule_id == "DQ_002":

                            key_columns = (
                                self._get_key_columns(
                                    row_dict.get(
                                        "KEY_COLUMNS"
                                    )
                                )
                            )

                            if not key_columns:

                                raise ValueError(
                                    f"KEY_COLUMNS missing for "
                                    f"{rule_id}"
                                )

                            logger.info(
                                f"Executing DQ_002 duplicate "
                                f"check: "
                                f"CONFIG_ID={config_id}, "
                                f"KEY_COLUMNS={key_columns}"
                            )

                            (
                                failed_df,
                                failed_count,
                                rule_expression
                            ) = rule_func(
                                df,
                                None,
                                key_columns
                            )

                            column_name_for_report = (
                                column_key
                            )

                        # =========================================
                        # DQ_006 - VALID VALUES
                        # =========================================

                        elif rule_id == "DQ_006":

                            valid_values = (
                                row_dict.get(
                                    "VALID_VALUES"
                                )
                            )

                            (
                                failed_df,
                                failed_count,
                                rule_expression
                            ) = rule_func(
                                df,
                                column_name,
                                valid_values
                            )

                            column_name_for_report = (
                                column_name
                            )

                        # =========================================
                        # STANDARD RULE
                        # =========================================

                        else:

                            params = [
                                column_name
                            ]

                            if min_val is not None:

                                params.append(
                                    min_val
                                )

                            if max_val is not None:

                                params.append(
                                    max_val
                                )

                            (
                                failed_df,
                                failed_count,
                                rule_expression
                            ) = rule_func(
                                df,
                                *params
                            )

                            column_name_for_report = (
                                column_name
                            )

                        # =========================================
                        # QUERY ID
                        # =========================================

                        if query_history.queries:

                            query_id = (
                                query_history
                                .queries[-1]
                                .query_id
                            )

                    logger.info(
                        f"DQ rule calculation completed: "
                        f"RULE_ID={rule_id}, "
                        f"CONFIG_ID={config_id}, "
                        f"COLUMN={column_key}, "
                        f"FAILED_COUNT={failed_count}"
                    )

                except Exception as exc:

                    if (
                        query_history
                        and query_history.queries
                    ):

                        query_id = (
                            query_history
                            .queries[-1]
                            .query_id
                        )

                    error_message = str(exc)

                    logger.exception(
                        f"Error executing {rule_id} "
                        f"CONFIG_ID={config_id} "
                        f"COLUMN={column_key}: "
                        f"{error_message}"
                    )

                    # ---------------------------------------------
                    # ERROR REPORT
                    # ---------------------------------------------

                    try:

                        empty_failed_df = (
                            df.filter("1 = 0")
                        )

                        self.reporter.generate_error_report(
                            rule_id=rule_id,
                            rule_type=rule_name,
                            database=database,
                            schema=schema,
                            table=table,
                            config_id=config_id,
                            column_name=column_key,
                            rule_expression=rule_expression,
                            threshold=threshold,
                            severity=severity,
                            total_count=total_count,
                            failed_count=0,
                            start_time=start_time,
                            executed_by="DQ_TOOL",
                            source_table=full_table_name,
                            failed_df=(
                                failed_df
                                if failed_df is not None
                                else empty_failed_df
                            ),
                            error_message=error_message,
                            query_id=query_id
                        )

                    except Exception as report_error:

                        logger.exception(
                            f"Failed to generate error report "
                            f"for {rule_id}, "
                            f"CONFIG_ID={config_id}: "
                            f"{report_error}"
                        )

                    rules_executed += 1
                    fail_count += 1

                    if severity == "HIGH":

                        critical_fail_count += 1

                    continue

                # =================================================
                # GENERATE DQ RESULT
                # =================================================

                report_exception = None
                rule_status = None

                try:

                    logger.info(
                        f"Generating DQ result for "
                        f"{rule_id}, "
                        f"CONFIG_ID={config_id}, "
                        f"COLUMN={column_name_for_report}"
                    )

                    rule_status = (
                        self.reporter.generate_report(
                            rule_id=rule_id,
                            rule_type=rule_name,
                            database=database,
                            schema=schema,
                            table=table,
                            config_id=config_id,
                            column_name=(
                                column_name_for_report
                            ),
                            rule_expression=(
                                rule_expression
                            ),
                            threshold=threshold,
                            severity=severity,
                            total_count=total_count,
                            failed_count=failed_count,
                            start_time=start_time,
                            executed_by="DQ_TOOL",
                            source_table=full_table_name,
                            failed_df=failed_df,
                            query_id=query_id
                        )
                    )

                    logger.info(
                        f"Completed DQ result generation: "
                        f"{rule_id}, "
                        f"CONFIG_ID={config_id}, "
                        f"RESULT_STATUS={rule_status}"
                    )

                except Exception as report_error:

                    report_exception = report_error

                    logger.exception(
                        f"Report generation returned an error: "
                        f"RULE_ID={rule_id}, "
                        f"CONFIG_ID={config_id}, "
                        f"COLUMN={column_key}, "
                        f"ERROR={report_error}"
                    )

                # =================================================
                # VERIFY DQ RESULT
                # =================================================

                result_exists = (
                    self.is_result_available(
                        config_id=config_id,
                        rule_id=rule_id,
                        column_name=column_name_for_report
                    )
                )

                # =================================================
                # WRITE EXECUTION LOG
                # =================================================

                if result_exists:

                    logger.info(
                        f"DQ_RESULT exists for "
                        f"RULE_ID={rule_id}, "
                        f"CONFIG_ID={config_id}, "
                        f"COLUMN={column_key}. "
                        "Writing execution log as COMPLETED."
                    )

                    log_success = (
                        self.log_execution(
                            database=database,
                            schema=schema,
                            table=table,
                            rule_id=rule_id,
                            config_id=config_id,
                            column_name=column_key
                        )
                    )

                    if log_success:

                        logger.info(
                            f"DQ execution successfully completed: "
                            f"RULE_ID={rule_id}, "
                            f"CONFIG_ID={config_id}, "
                            f"COLUMN={column_key}, "
                            "STATUS=COMPLETED"
                        )

                    else:

                        logger.error(
                            f"DQ_RESULT exists but execution log "
                            f"could not be inserted: "
                            f"RULE_ID={rule_id}, "
                            f"CONFIG_ID={config_id}, "
                            f"COLUMN={column_key}"
                        )

                else:

                    logger.error(
                        f"DQ_RESULT was not found after report "
                        f"generation: "
                        f"RULE_ID={rule_id}, "
                        f"CONFIG_ID={config_id}, "
                        f"COLUMN={column_key}. "
                        "Execution will NOT be marked COMPLETED."
                    )

                    # ---------------------------------------------
                    # Generate error report because DQ_RESULT
                    # was not successfully persisted.
                    # ---------------------------------------------

                    try:

                        empty_failed_df = (
                            df.filter("1 = 0")
                        )

                        self.reporter.generate_error_report(
                            rule_id=rule_id,
                            rule_type=rule_name,
                            database=database,
                            schema=schema,
                            table=table,
                            config_id=config_id,
                            column_name=column_key,
                            rule_expression=rule_expression,
                            threshold=threshold,
                            severity=severity,
                            total_count=total_count,
                            failed_count=failed_count,
                            start_time=start_time,
                            executed_by="DQ_TOOL",
                            source_table=full_table_name,
                            failed_df=(
                                failed_df
                                if failed_df is not None
                                else empty_failed_df
                            ),
                            error_message=(
                                str(report_exception)
                                if report_exception
                                else
                                "DQ_RESULT was not created."
                            ),
                            query_id=query_id
                        )

                    except Exception as error_report_exception:

                        logger.exception(
                            f"Failed to generate error report "
                            f"for {rule_id}, "
                            f"CONFIG_ID={config_id}: "
                            f"{error_report_exception}"
                        )

                    rules_executed += 1
                    fail_count += 1

                    if severity == "HIGH":

                        critical_fail_count += 1

                    continue

                # =================================================
                # COUNT EXECUTION
                # =================================================

                rules_executed += 1

                # =================================================
                # DQ RESULT METRICS
                # =================================================

                if rule_status == "PASS":

                    pass_count += 1

                else:

                    fail_count += 1

                    if severity == "HIGH":

                        critical_fail_count += 1

        # ========================================================
        # FINAL SUMMARY
        # ========================================================

        logger.info(
            "DQ execution completed. "
            f"Tables Checked={tables_checked}, "
            f"Rules Executed={rules_executed}, "
            f"Passed={pass_count}, "
            f"Failed={fail_count}, "
            f"Critical Failures={critical_fail_count}"
        )

        return (
            tables_checked,
            rules_executed,
            pass_count,
            fail_count,
            critical_fail_count
        )