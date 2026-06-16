from snowflake.snowpark.functions import col
from src.checks import completeness, uniqueness, validity, custom_sql
import json


class DBConfigLoader:

    def __init__(self, session):
        self.session = session


    # --------------------------------------------------
    # LOAD ACTIVE RULE CONFIGURATION
    # --------------------------------------------------
    def load_active_rules(self):

        with open("config/dq_config.json", "r") as f:
            dq_config = json.load(f)
        
        print(type(dq_config))
        print(dq_config[:2])

        active_rules = [
            rule
            for rule in dq_config
            if rule.get("IS_ACTIVE", False)
        ]

        return active_rules


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
            },
            
            "DQ_006": {
                "func": validity.execute_valid_value_check,
                "name": "VALID_VALUE_CHECK"
            }
        }