/*
Sample query to extract metrics from resulting benchmark metrics Delta table
*/

SELECT
    run_timestamp,
    benchmark_catalog,
    benchmark_schema,
    id,
    query_text,
    warehouse_name,
    duration/1000 as duration_secs,
    metrics.planning_time_ms/1000 as query_planning_time_secs,
    metrics.execution_time_ms/1000 as query_execution_time_secs,
    metrics.compilation_time_ms/1000 as query_compliation_time_secs,
    metrics.result_from_cache as result_from_cache,
    metrics.rows_produced_count as rows_produced_count,
    metrics.read_files_count as read_files_count,
    metrics.read_partitions_count as read_partitions_count,
    metrics.pruned_bytes as pruned_bytes,
    metrics.pruned_files_count as pruned_files_count,
    metrics.spill_to_disk_bytes as spill_to_disk_bytes,
    metrics.rows_read_count as rows_read_count
FROM <metimur_metrics_table_name>
order by run_timestamp desc, id

/*
Sample query to extract cost per SQL warehouse involved in the benchmark
https://docs.databricks.com/en/admin/system-tables/index.html
*/

SELECT
  usage_metadata.warehouse_id,
  sku_name,
  usage_start_time,
  usage_end_time,
  usage_date,
  custom_tags,
  usage_unit,
  usage_quantity,
  CASE
    WHEN sku_name LIKE '%SERVERLESS%' THEN 0.7
    WHEN sku_name LIKE '%PRO%' THEN 0.55
    WHEN sku_name LIKE '%CLASSIC%' THEN 0.22
  END AS unit_cost,
  0.00 AS discount,
  usage_quantity * unit_cost * (1-discount) AS total_dollar_amount
FROM
  system.billing.usage
WHERE
  sku_name LIKE '%SQL%'
  AND usage_metadata.warehouse_id IN (
    SELECT DISTINCT warehouse_id
    FROM _metimur_metrics1_anhhoang_chu_databricks_com
  );

    
/*
Sample query to extract cost per query, require permission to databricks systems table 
https://docs.databricks.com/en/admin/system-tables/index.html
*/

with dbu_size_view AS
(
  SELECT '2X-Small' AS size, 4 AS dbu_per_h
  UNION ALL
  SELECT 'X-Small', 6
  UNION ALL
  SELECT 'Small', 12
  UNION ALL
  SELECT 'Medium', 24
  UNION ALL
  SELECT 'Large', 40
  UNION ALL
  SELECT 'X-Large', 80
  UNION ALL
  SELECT '2X-Large', 144
  UNION ALL
  SELECT '3X-Large', 272
  UNION ALL
  SELECT '4X-Large', 528
),
 
system_warehouse_cte as (
SELECT
    we.warehouse_id AS warehouse_id,
    we.event_type AS event,
    we.event_time AS event_time,
    we.cluster_count AS cluster_count,
    lead(event_time) over (PARTITION BY warehouse_id ORDER BY event_time) AS next_event_time
FROM
    system.compute.warehouse_events AS we
WHERE
    we.event_type IN (
        'STARTING', 'RUNNING', 'STOPPING', 'STOPPED',
        'SCALING_UP', 'SCALED_UP', 'SCALING_DOWN', 'SCALED_DOWN'
    )
)
 
select a. run_timestamp, a.benchmark_catalog, a.benchmark_schema,
  a.warehouse_id, a.id, a.warehouse_name, a.warehouse_size,
  a.duration/(1000*60*60) as duration_hours,
  b.cluster_count, c.dbu_per_h,
  a.duration/(1000*60*60) * cluster_count * c.dbu_per_h as dollar_cost
from (select *, split_part(warehouse_name, " ", -1) as warehouse_size from <metimur_metrics_table>) a
join system_warehouse_cte b
join dbu_size_view c
on  a.warehouse_id = b.warehouse_id
and a.warehouse_size = c.size
and from_unixtime(a.query_start_time_ms/1000) between b.event_time and b.next_event_time
;
