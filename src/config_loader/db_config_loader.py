from snowflake.snowpark.functions import col
from src.checks import completeness, uniqueness, validity, custom_sql


class DBConfigLoader:

    def __init__(self, session):
        self.session = session


    # --------------------------------------------------
    # LOAD ACTIVE RULE CONFIGURATION
    # --------------------------------------------------
    def load_active_rules(self):

        dq_config_df = (
            self.session
            .table("DEMO_DB.PUBLIC.DQ_CONFIG")
            .filter(col("IS_ACTIVE") == True)
        )

        return dq_config_df


    # --------------------------------------------------
    # LOAD RULE LOOKUP (FUNCTION + RULE NAME)
    # --------------------------------------------------
    def load_rule_lookup(self):

        return {

            "DQ_001": {
                "func": completeness.execute,
                "name": "NOT_NULL_CHECK"
            },

            "DQ_002": {
                "func": uniqueness.execute,
                "name": "UNIQUE_CHECK"
            },

            "DQ_003": {
                "func": validity.execute_range,
                "name": "RANGE_CHECK"
            },

            "DQ_004": {
                "func": validity.execute_min_length,
                "name": "MIN_LENGTH_CHECK"
            },

            "DQ_005": {
                "func": custom_sql.execute,
                "name": "CUSTOM_SQL_CHECK"
            }
        }