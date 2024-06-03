-- start_ignore
SELECT s.groupid, s.num_running, s.num_queueing, s.num_queued, s.num_executed
FROM pg_resgroup_get_status(NULL::oid) s(groupid, num_running, num_queueing, num_queued, num_executed, total_queue_duration, cpu_usage, memory_usage);
-- end_ignore
CREATE TEMP TABLE resgroup_function_test(LIKE gp_toolkit.gp_resgroup_status);

INSERT INTO resgroup_function_test(groupid, num_running, num_queueing, num_queued, num_executed)
SELECT s.groupid, s.num_running, s.num_queueing, s.num_queued, s.num_executed
FROM pg_resgroup_get_status(NULL::oid) s(groupid, num_running, num_queueing, num_queued, num_executed, total_queue_duration, cpu_usage, memory_usage) LIMIT 1;

INSERT INTO resgroup_function_test(groupid, num_running, num_queueing, num_queued, num_executed)
SELECT (unnest).groupid, (unnest).num_running, (unnest).num_queueing, (unnest).num_queued, (unnest).num_executed 
FROM (
  SELECT unnest(array(
    SELECT row('', groupid, num_running, num_queueing, num_queued, num_executed, total_queue_duration, cpu_usage, memory_usage)::resgroup_function_test
    FROM pg_resgroup_get_status(NULL::oid)
	LIMIT 1
  ))
) a;

SELECT count(num_executed)>0 FROM resgroup_function_test WHERE num_executed IS NOT NULL;

CREATE TEMP TABLE tst_json (cpu_usage json);

INSERT INTO tst_json
SELECT * FROM unnest(array(
    SELECT cpu_usage
    FROM pg_resgroup_get_status(NULL::oid)
));
