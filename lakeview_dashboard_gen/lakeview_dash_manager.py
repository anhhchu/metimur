# Databricks notebook source
import base64
import json
import re

import requests


class lakeview_dash_manager:
    """Lakeview Dashboard API class
    """
    def __init__(self, host, token):
        self.host = host
        self.token = token
        self.url_base = f"https://{self.host}/api/2.0/workspace"
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.lakeview_json = None

    def list_content(self, path):
        """List out the content from the path of a workspace

        Args:
            path: workspace path to list content from

        Returns:
            a json object with the content of the workspace path
        """
        url = f"{self.url_base}/list"
        data = {"path": path}
        response = requests.get(url, headers=self.headers, json=data)
        return response.json()

    def export_dash(self, path, dashboard_name):
        """export dashboard with given workspace path and dashboard name

        Args:
            path): workspace path to export lakeview dashboard from
            dashboard_name: lakeview dashboard name to export
        """
        url = f"{self.url_base}/export"
        path = f"{path}/{dashboard_name}.lvdash.json"
        data = {"path": path, "direct_download": True}
        response = requests.get(url, headers=self.headers, json=data)
        self.lakeview_json = json.loads(response.text)
        return

    def import_dash(self, path, dashboard_name):
        """_summary_

        Args:
            path: workspace path to import lakeview dashboard to
            dashboard_name: lakeview dashboard name to write to

        Returns:
            api resonse (expect to be 200 for success)
        """
        url = f"{self.url_base}/import"
        path_full = f"{path}/{dashboard_name}.lvdash.json"
        data = {
            "content": self.basee64_encode(json.dumps(self.lakeview_json)),
            "path": path_full,
            "overwrite": True,
            "format": "AUTO",
        }
        response = requests.post(url, headers=self.headers, json=data)
        if response.status_code != 200:
            raise Exception(f"Unexpected status code: {response.status_code}")
        return self.get_dashboard_links(path, dashboard_name)
    
    def save_dash_local(self, path):
        """save lakeview dashboard json to local file

        Args:
            path: path to save the lakeview dashboard to
        """
        with open(path, "w") as f:
            json.dump(self.lakeview_json, f, indent=4)
        return
    
    def load_dash_local(self, path):
        """load lakeview dashboard template from local file

        Args:
            path: path to load the lakeview dashboard tempate from
        """
        with open(path, "r") as f:
            self.lakeview_json = json.load(f)

    def set_query_uc(self, catalog_name, schema_name, table_name):
        """update the catalog_name, schema_name, table_name in the lakeview dashboard json obj

        Args:
            catalog_name: catalog_name in string
            schema_name: schema_name in string
            table_name: metrics table_name in string
        """
        for item in self.lakeview_json["datasets"]:
            item["query"] = re.sub(r"CATALOG_NAME", catalog_name, item["query"])
            item["query"] = re.sub(r"SCHEMA_NAME", schema_name, item["query"])
            item["query"] = re.sub(r"TABLE_NAME", table_name, item["query"])

    def get_dashboard_links(self, path, dashboard_name):
        """Get the dashboard_id based on workspace path of a dashboard
        http link follows the format of {workspace_host}/sql/dashboardsv3/{dashboard_id}
        
        Args:
            path: workspace path in string
            dashboard_name: lakeview dashboard name to write to

        Return: 
            dashboard http links
        """
        url = f"{self.url_base}/get-status"
        path_full = f"{path}/{dashboard_name}.lvdash.json"
        data = {
            "path": path_full,
        }
        response = requests.get(url, headers=self.headers, json=data)
        dashboard_id = response.json().get('resource_id')
        return f"https://{self.host}/sql/dashboardsv3/{dashboard_id}"

    @staticmethod
    def basee64_encode(x):
        """base64 encode a string

        Args:
            x: string to encode

        Returns:
            endcoded string
        """
        encoded_bytes = base64.b64encode(x.encode("utf-8"))
        encoded_string = encoded_bytes.decode("utf-8")
        return encoded_string
