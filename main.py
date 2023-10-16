import os
import asyncio
from typing import Dict, List, Optional


import utils
from snowflake_connector import SnowflakeConnector


def main():
    query_list: List[str] = [
        query for query in os.getenv("INPUT_QUERIES", "").split(";") if query != ""
    ]
    # by default run all queries async
    exec_sync = False if "false" in os.getenv("INPUT_SYNC", "false").lower() else True
    snowflake_warehouse: str = os.getenv("INPUT_SNOWFLAKE_WAREHOUSE")
    snowflake_account: str = os.getenv("INPUT_SNOWFLAKE_ACCOUNT")
    snowflake_username: str = os.getenv("INPUT_SNOWFLAKE_USERNAME")
    snowflake_password: str = os.getenv("INPUT_SNOWFLAKE_PASSWORD")
    snowflake_role: Optional[str] = os.getenv("INPUT_SNOWFLAKE_ROLE", None)

    with SnowflakeConnector(
        snowflake_account,
        snowflake_username,
        snowflake_password,
        snowflake_warehouse,
        snowflake_role,
    ) as con:
        json_results: Dict[str, List] = con.execute_and_fetch(query_list, exec_sync)
        utils.set_github_action_output("queries_results", json_results)


if __name__ == "__main__":
    main()
