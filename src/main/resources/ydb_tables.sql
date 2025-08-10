create table scheduled_tasks (
    task_name Text NOT NULL,
    task_instance Text NOT NULL,
    task_data String,
    execution_time Timestamp,
    picked Boolean,
    picked_by Text,
    last_success Timestamp,
    last_failure Timestamp,
    consecutive_failures Int32,
    last_heartbeat Timestamp,
    version Int64,
    priority Int32,
    PRIMARY KEY (task_name, task_instance)
);

