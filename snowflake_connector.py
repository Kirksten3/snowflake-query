import asyncio
from typing import Dict, Generator, List, Tuple
from collections import ChainMap
import json

import snowflake.connector

from snowflake.connector.constants import QueryStatus


def query_print_detail(fn):
    def wrapper(self, query_str) -> str:
        query_id = fn(self, query_str)
        print("### Query run start ###")
        print(f"[!] Query id - {query_id}")
        print(f"[!] Running query - {query_str}")
        return query_id

    return wrapper


class SnowflakeConnector:
    def __init__(
        self,
        account_name: str,
        username: str,
        password: str,
        warehouse: str,
        role: str = None,
    ):
        self.account_name = account_name
        self.username = username
        self.password = password
        self.warehouse = warehouse
        self.role = role
        self.con: snowflake.connector.SnowflakeConnection = None
        self.cursor: snowflake.connector.cursor.SnowflakeCursor = None

    def __enter__(self):
        self.con = snowflake.connector.connect(
            user=self.username, password=self.password, account=self.account_name
        )
        self.cursor = self.con.cursor()
        if self.role:
            self.cursor.execute(f"USE ROLE {self.role}")
        self.cursor.execute(f"USE WAREHOUSE {self.warehouse}")
        return self

    def __exit__(self, *exc):
        self.cursor.close()
        self.con.close()

    @query_print_detail
    def query(self, query_str: str) -> str:
        self.cursor.execute_async(query_str)
        return self.cursor.sfqid

    def is_query_running(self, query_id):
        return self.con.get_query_status(query_id) in [
            QueryStatus.RUNNING,
            QueryStatus.NO_DATA,
        ]

    def _fetch_query_results(self, query_id) -> List[tuple]:
        self.cursor.get_results_from_sfqid(query_id)
        return self.cursor.fetchall()

    async def fetch_query_results_async(self, query_id) -> List[tuple]:
        while self.is_query_running(query_id):
            await asyncio.sleep(1)
        return self._fetch_query_results(query_id)

    async def fetch_multiple_query_results_async(self, query_ids: List[str]):
        results: Dict[str, List[str]] = {}
        running_tasks = {
            asyncio.create_task(self.fetch_query_results_async(query_id), name=query_id)
            for query_id in query_ids
        }
        while len(running_tasks) != 0:
            completed_tasks, running_tasks = await asyncio.wait(
                running_tasks, return_when=asyncio.FIRST_COMPLETED
            )

        for completed_task in completed_tasks:
            results[completed_task.get_name()] = [
                row for row in completed_task.result()
            ]

        return results

    def execute_and_fetch(
        self, query_list: List[str], run_sync=False
    ) -> Dict[str, List]:
        """This is a fun way of using the same exec path to run queries sync or async.
        For the synchronous path, ChainMap is used to condense all the individual
        dictionaries that are generated. There's probably more overhead that way
        but this dramatically simplifies code path.

        Args:
            query_list (List[str]): A list of queries to run.
            run_sync (bool, optional): Whether or not to run queries synchronously. Defaults to False.

        Returns:
            Dict[str, List]: A dictionary of query_id:List[query_results]
        """
        def _execute(queries: List[str]) -> Dict[str, List]:
            return asyncio.run(
                self.fetch_multiple_query_results_async(
                [self.query(query) for query in queries]
                ))
        if run_sync:
            return dict(ChainMap(*[_execute([query]) for query in query_list]))
        return _execute(query_list)
