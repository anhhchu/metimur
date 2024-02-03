from utils.parse_queries import get_queries_from_file_format_semi, get_queries_from_file_format_orig

import json
import os
from urllib.parse import urljoin, urlencode
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

import pyarrow
import requests
import time
import logging
import pandas as pd


# NOTE set debuglevel = 1 (or higher) for http debug logging
from http.client import HTTPConnection
HTTPConnection.debuglevel = 0

from dotenv import load_dotenv
load_dotenv(".env")

HOST = os.getenv("HOST")
URL = os.getenv("URL")
CATALOG = os.getenv("CATALOG")
SCHEMA = os.getenv("SCHEMA")
USER_NAME = os.getenv("USER_NAME")

if HOST and not URL:
    URL = f"https://{HOST}/api/2.0/sql/statements/"

WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")
AUTH_TOKEN = os.environ.get("AUTH_TOKEN")
assert URL and WAREHOUSE_ID and AUTH_TOKEN, "Required: HOST||URL, WAREHOUSE_ID, and AUTH_TOKEN"

headers = {
    'Content-Type': 'application/json'
}
auth=('token', AUTH_TOKEN)

import re



def _get_user_id(username):
    response = requests.get(
    f"https://{HOST}/api/2.0/preview/scim/v2/Users?filter=userName+eq+{username}",
    headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
    )
    user_id = json.loads(response.text)["Resources"][0]["id"] 
    return user_id

def execute_single_query(query, parameters=[]):

    payload = json.dumps({
        "statement": query[0],
        "warehouse_id": WAREHOUSE_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "parameters": parameters,
        "wait_timeout": "50s",
        "disposition":
          "EXTERNAL_LINKS",
        "format": "ARROW_STREAM"
    })

    response = requests.post(URL, auth=auth, headers=headers, data=payload)
    return response

def wait_for_completion(response):
    assert response.status_code == 200, print(response.json())
    state = response.json()["status"]["state"]
    statement_id = response.json()["statement_id"]
    while state in ["PENDING", "RUNNING"]:
        stmt_url = urljoin(URL, statement_id)
        response = requests.get(stmt_url, auth=auth, headers=headers)
        # print("Statement GET got HTTP status code:", response.status_code)
        assert response.status_code == 200
        state = response.json()["status"]["state"]
    print(state)
    assert state == "SUCCEEDED"
    return response


def process_success_csv(response, filedir='download'):
    chunks = response.json()["manifest"]["chunks"]
    statement_id = response.json()["statement_id"]

    # Create results directory if it doesn't exist
    if not os.path.exists(filedir):
        os.makedirs(filedir)

    print(f"{len(chunks)} chunks(s) in result set")

    df = pd.DataFrame()
    for idx, chunkInfo in enumerate(chunks):
        stmt_url = urljoin(URL, statement_id) + "/"
        row_offset_param = urlencode({'row_offset': chunkInfo["row_offset"]})
        resolve_external_link_url = urljoin(stmt_url, "result/chunks/{}?{}".format(
            chunkInfo["chunk_index"], row_offset_param))

        response = requests.get(resolve_external_link_url, auth=auth, headers=headers)
        assert response.status_code == 200

        external_url = response.json()["external_links"][0]["external_link"]
        # NOTE: do _NOT_ send the authorization header to external urls
        raw_response = requests.get(external_url, auth=None, headers=None)
        assert raw_response.status_code == 200

        arrow_table = pyarrow.ipc.open_stream(raw_response.content).read_all()
        
        df = arrow_table.to_pandas()

        # Write to CSV
        print(f"Write chunk to {filedir}/file_{idx}.csv")
        df.to_csv(f'{filedir}/file_{idx}.csv', index=False)

        print("chunk {} received".format(idx))
    
def preWarmTables(tables):
    print("Pre-warming tables")
    for table in tables:
        q = (f"cache select * from {table}", "q0")
        response = execute_single_query(q)


def get_query_history(warehouse_id, start_ts_ms, end_ts_ms, user_id=None):
    """
    Retrieves the Query History for a given workspace and Data Warehouse.

    Parameters:
    -----------
    warehouse_id (str): The ID of the Data Warehouse for which to retrieve the Query History.
    start_ts_ms (int): The start timestamp in milliseconds for the query history range.
    end_ts_ms (int): The end timestamp in milliseconds for the query history range.
    user_id (str, optional): The ID of the user for which to retrieve the Query History. Defaults to None.

    Returns:
    --------
    list of dict: A list of dictionaries where each dictionary represents a query history item. Each dictionary contains 
    information about a single query such as the query text, start time, end time, and other metrics.

    Raises:
    ------
    Exception: If the request to the API fails or the response does not contain the 'res' key, an exception is raised with the response content.

    """
    ## Put together request 
    payload = json.dumps({
        "filter_by": {
            "query_start_time_range": {
                "end_time_ms": end_ts_ms,
                "start_time_ms": start_ts_ms
        },
        "warehouse_ids": warehouse_id,
        },
        "user_ids": [user_id],
        "include_metrics": "true",
        "max_results": "1000"
    })


    uri = f"https://{HOST}/api/2.0/sql/history/queries"
    headers_auth = {"Authorization":f"Bearer {AUTH_TOKEN}"}

    #### Get Query History Results from API
    response = requests.get(uri, data=payload, headers=headers_auth)
    if (response.status_code == 200) and ("res" in response.json()):
        logging.info("Query history extracted successfully")
        while True:
            results = response.json()['res']
            if all([item['is_final'] for item in results]):
                break
            time.sleep(5)
            response = requests.get(uri, data=payload, headers=headers_auth)
        end_res = response.json()['res']
        return end_res
    else:
        raise Exception(response.json())


def run_queries(query_file, params, concurrency=1):
    queries = get_queries_from_file_format_orig(query_file)
    statements = []
    for query in queries: 
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(execute_single_query, query, param) for param in params]
            wait(futures, return_when=ALL_COMPLETED)
        statements += ([(query[1], future.result().json()["statement_id"]) for future in futures])
    return statements


def benchmark(query_file, params, concurrency=1):
    start_ts_ms = int(time.time() * 1000)

    statement_ids = []
    for i in range(0, len(params), concurrency):
        statement_ids += (run_queries(query_file, params[i:i+concurrency]))


    end_ts_ms = int(time.time() * 1000)
    print(start_ts_ms, end_ts_ms)
    history = get_query_history(warehouse_id=WAREHOUSE_ID, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms, user_id=_get_user_id(USER_NAME))

    df = pd.DataFrame(history)

    statement_df = pd.DataFrame(statement_ids, columns=["query", "query_id"])

    # Merge df with statement_df on query_id
    merged_df = pd.merge(df, statement_df, left_on='query_id', right_on='query_id')

    # Group by query and calculate the average of duration and metrics.query_execution_time_ms
    metrics = merged_df.groupby('query').agg({'duration': 'mean'})

    return metrics

if __name__ == "__main__":

    ############################################################################################################
    ### To download the results of a query to csv file, uncomment the following lines

    # query = ("select * from customer", "q0")

    # response = execute_single_query(query, {})

    # process_success_csv(response, 'download')

    ############################################################################################################
    ### To benchmark queries, uncomment the following lines

    query_file = "queries/tpch_w_param.sql"
    concurrency = 3

    dates = ["1998-12-01"]
    regions = ["AMERICA", "ASIA", "EUROPE"]

    params = [[{"name":"my_date", "value":d}, {"name":"region", "value":r}] for d in dates for r in regions]

    metrics = benchmark(query_file, params, concurrency )

    print(metrics)
