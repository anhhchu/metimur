# Databricks notebook source
import os
import warnings
from typing import Dict

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute, sql
from databricks.sdk.service.compute import AutoScale, ClusterSource, ClusterSpec, DataSecurityMode,RuntimeEngine, ClusterLogConf, DbfsStorageInfo,InitScriptInfo, WorkspaceStorageInfo


class DatabricksClient:
    def __init__(self, hostname, token, constants: dict):
        # Instantiate the python SDK client
        self.w = WorkspaceClient(host=f"https://{hostname}", token=token)

        # Expose constants that are referenced by methods
        self.constants = constants

        # Define hard-coded properties that are referenced by methods
        self._latest_spark_version: str = None
        self._cloud_specific_cluster_type: str = None
        self._number_of_cores_per_worker: int = None
        self._warehouse_prefix: str = None

    ################# Class Properties for Cluster Configs ############
    @property
    def latest_spark_version(self) -> str:
        return self.w.clusters.select_spark_version(latest=True, long_term_support=True)

    @property
    def cloud_specific_cluster_type(self) -> str:
        if self.w.config.is_azure:
            return "Standard_DS3_v2"
        elif self.w.config.is_gcp:
            return "n1-highmem-4"
        elif self.w.config.is_aws:
            return "i3.xlarge"
        else:
            raise ValueError(
                f"w.config did not return one of the three supported clouds"
            )

    @property
    def number_of_cores_per_worker(self) -> int:
        return 4
    
    @property
    def base_cluster_config(self) -> Dict:
        return {
            "enable_local_disk_encryption": False,
            "runtime_engine": "PHOTON",
            "node_type_id": self.cloud_specific_cluster_type,
            "single_user_name": f"{self.constants.current_user_email}",
            "data_security_mode": "SINGLE_USER",
        }

    
    ########### Cluster Configurations ###########
    def _get_data_generator_cluster_config(self) -> Dict:
        additional_configs = {
            "name": "Data Generation Cluster",
            "num_workers": self.constants.workers_scale_factor_map,
            # Specify singe_user because scala requires a single user mode
            "spark_version": "12.2.x-scala2.12",
        }

        return self.base_cluster_config | additional_configs

    def _get_load_testing_cluster_config(self) -> Dict:

        additional_configs = {
            "name": "Benchmarking Cluster",
            "num_workers": 1,
            "spark_version": self.latest_spark_version,
        }

        return self.base_cluster_config | additional_configs
    

    ################# Create Data Functions ###############
    def create_job(self):
        if self.constants.benchmarks == "BYOD":
            step_1 = {
                "task_key": "generate_data",
                "notebook_task": {
                    "notebook_path": self.constants.datagen_notebook_path,
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "schema_path": schema_path
                    },
                },
                # "job_cluster_key": "metimur_cluster"
                "new_cluster": self._get_data_generator_cluster_config(),
                # "existing_cluster_id": c.cluster_id
            }
        elif self.constants.benchmarks in ("TPCDS", "TPCH"):
            step_1 = {
                "task_key": "generate_data",
                "notebook_task": {
                    "notebook_path": self.constants.datagen_notebook_path,
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "benchmarks": self.constants.benchmarks,
                        # "catalogName": catalog_name,
                        "schemaName": schema_name,
                        "baseLocation": f"dbfs:/Serverless_Benchmark",
                        "scaleFactors": self.constants.scale_factors,
                        "fileFormat": "delta",
                        "overwrite": "true",
                        "createTableStats": "true",
                        "partitionTables": partition_tables,
                    },
                },
                # "job_cluster_key": "metimur_cluster"
                "new_cluster": self._get_data_generator_cluster_config(),
                # "existing_cluster_id": c.cluster_id
            }

        step_2 = {
            "task_key": "run_benchmarking",
            # "depends_on": [{"task_key": "generate_data"}],
            "notebook_task": {
                "notebook_path": self.constants.run_benchmark_notebook_path,
                "source": "WORKSPACE",
                "base_parameters": {
                    "benchmark_choice": self.constants.benchmark_choice,
                    "warehouse_prefix": self.constants.warehouse_prefix,
                    "warehouse_sizes": self.constants.warehouse_sizes,
                    "warehouse_type": self.constants.warehouse_type,
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "query_path": self.constants.query_path,
                    "params_path": self.constants.params_path,
                    "query_repetition_count": self.constants.query_repetition_count,
                    "concurrency": self.constants.concurrency,
                    "max_clusters": self.constants.min_clusters,
                    "max_clusters": self.constants.max_clusters,
                    "results_cache_enabled": self.constants.results_cache_enabled,
                    "disk_cache_enabled": self.constants.disk_cache_enabled,
                },
            },
            # "job_cluster_key": "metimur_cluster",
            "new_cluster": self._get_load_testing_cluster_config(),
        }

        if tables_already_exist: 
            job = self.w.jobs.create(
                    # job_clusters=[jobs.JobCluster(
                    #       job_cluster_key="metimur_cluster" , 
                    #       new_cluster=ClusterSpec.from_dict(self._get_load_testing_cluster_config()))],
                    name=self.constants.job_name,
                    run_as=jobs.JobRunAs(user_name=self.constants.current_user_email),
                    tasks=[
                        # jobs.Task.from_dict(step_1),
                        jobs.Task.from_dict(step_2),
                    ],
                )
        else:
            step_2["depends_on"] = [{"task_key": "generate_data"}]
            job = self.w.jobs.create(
                # job_clusters=[jobs.JobCluster(
                #       job_cluster_key="metimur_cluster" , 
                #       new_cluster=ClusterSpec.from_dict(self._get_data_generator_cluster_config()))],
                name=self.constants.job_name,
                run_as=jobs.JobRunAs(user_name=self.constants.current_user_email),
                tasks=[
                    jobs.Task.from_dict(step_1),
                    jobs.Task.from_dict(step_2),
                ],
            )

        return job


    def run_job(self, job_id: str):
        return self.w.jobs.run_now(**{"job_id": job_id})
    

    def clean_job(self, job_id:str):
        self.w.jobs.delete(job_id=job_id)

