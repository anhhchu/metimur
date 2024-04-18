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
        """_summary_

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
        """_summary_

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
        path = f"{path}/{dashboard_name}.lvdash.json"
        data = {
            "content": self.basee64_encode(json.dumps(self.lakeview_json)),
            "path": path,
            "overwrite": True,
            "format": "AUTO",
        }
        response = requests.post(url, headers=self.headers, json=data)
        return response.json()
    
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

    def set_query_uc(self, catalog_name, schema_name):
        """update the catalog_name and schema_name in the lakeview dashboard json obj

        Args:
            catalog_name: catalog_name in string
            schema_name: schema_name in string
        """
        for item in self.lakeview_json["datasets"]:
            item["query"] = re.sub(r"CATALOG_NAME", catalog_name, item["query"])
            item["query"] = re.sub(r"SCHEMA_NAME", schema_name, item["query"])

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
