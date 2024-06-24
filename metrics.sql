SELECT
    run_timestamp,
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
FROM <metrics_table_name>
order by run_timestamp desc, id
