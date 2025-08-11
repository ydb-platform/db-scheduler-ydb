# db-scheduler-ydb

Module adding [YDB](https://ydb.tech) support for Gustav Karlsson's [db-scheduler](https://github.com/kagkarlsson/db-scheduler).

## Principle

This module provides 3 main components :

- `YdbTaskRepository`, a YDB implementation of `TaskRepository`
- `YdbSchedulerBuilder`, a builder to make it easy to create a scheduler with the YDB repository
- `YdbScheduler`, a scheduler extending the `Scheduler`, particularly the `create` method

This module aims to be as non-intrusive as possible regarding the main project modules.

## Usage

1. Add maven dependency :
 ```xml
<dependency>
    <groupId>tech.ydb</groupId>
    <artifactId>db-scheduler-ydb</artifactId>
    <version>9.4.1</version>
</dependency>
```

Artifact version numbers are aligned with the main project's version numbers, with the extra patch number appended.

2. Add the tasks table to your YDB database:

```sql
CREATE TABLE scheduled_tasks (
    task_name Text NOT NULL,
    task_instance Text NOT NULL,
    task_data String,
    execution_time Timestamp,
    picked Bool,
    picked_by Text,
    last_success Timestamp,
    last_failure Timestamp,
    consecutive_failures Int32,
    last_heartbeat Timestamp,
    version Int64,
    priority Int32,
    PRIMARY KEY (task_name, task_instance)
);
```

3. Instantiate and start the scheduler

```java
GrpcTransport gt = GrpcTransport.forConnectionString(connectionString()).build();
QueryClient qc = QueryClient.newClient(gt).build();
Scheduler scheduler = YdbScheduler.create(qc, "db_scheduler_tasks", knownTasks).build();
scheduler.start();
```

4. Use the scheduler as detailed in the [db-scheduler documentation](https://github.com/kagkarlsson/db-scheduler/blob/master/README.md).
