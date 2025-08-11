package com.github.kagkarlsson.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;

/**
 * TaskRepository implementation for YDB data sources.
 * This code is heavily based on the original JdbcTaskRepository.
 *
 * @author mzinal
 */
public class YdbTaskRepository implements TaskRepository {
    private static final Logger LOG = LoggerFactory.getLogger(YdbTaskRepository.class);

    public static final String DEFAULT_TABLE_NAME = "scheduled_tasks";

    private static final OptionalType TYPE_TEXT = PrimitiveType.Text.makeOptional();
    private static final OptionalType TYPE_TS = PrimitiveType.Timestamp.makeOptional();

    private final TaskResolver taskResolver;
    private final SchedulerName schedulerSchedulerName;
    private final Serializer serializer;
    private final SessionRetryContext retryCtx;
    private final String tableName;

    public YdbTaskRepository(TaskResolver taskResolver,
        SchedulerName schedulerSchedulerName,
        Serializer serializer, String tableName, QueryClient ydbClient) {
        this.taskResolver = taskResolver;
        this.schedulerSchedulerName = schedulerSchedulerName;
        this.serializer = serializer;
        this.retryCtx = SessionRetryContext.create(ydbClient)
                .idempotent(true).build();
        this.tableName = tableName;
    }

    @Override
    public boolean createIfNotExists(Execution execution) {
        LOG.debug("Creation request for execution {}", execution);

        String queryText = "DECLARE $input AS Struct<task_name:Text,"
                + "task_instance:Text, task_data:String,"
                + "execution_time:Timestamp, picked:Boolean,"
                + "version:Int64>; "
                + ""
                + "SELECT 1 AS a FROM `" + tableName + "` "
                + "WHERE task_name=$input.task_name AND task_instance=$input.task_instance; "
                + ""
                + "UPSERT INTO `" + tableName + "` SELECT q.* FROM AS_TABLE([$input]) AS q "
                + "LEFT JOIN `" + tableName + "` AS t "
                + "ON q.task_name=t.task_name AND q.task_instance=t.task_instance "
                + "WHERE t.task_instance IS NULL;";

        byte[] taskData = serializer.serialize(execution.taskInstance.getData());

        HashMap<String, Value<?>> input = new HashMap<>();
        input.put("task_name", PrimitiveValue.newText(execution.taskInstance.getTaskName()));
        input.put("task_instance", PrimitiveValue.newText(execution.taskInstance.getId()));
        input.put("task_data", PrimitiveValue.newBytes(taskData));
        input.put("execution_time", PrimitiveValue.newTimestamp(execution.executionTime));
        input.put("picked", PrimitiveValue.newBool(false));
        input.put("version", PrimitiveValue.newInt64(1L));

        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(queryText, TxMode.SERIALIZABLE_RW,
                        Params.of("$input", StructValue.of(input)))))
                .join().getValue().getResultSet(0);
        if (result.next()) {
            LOG.debug("Execution not created, it already exists.");
            return false;
        }

        return true;
    }

    private YdbQueryBuilder queryForFilter(ScheduledExecutionsFilter filter) {
        final YdbQueryBuilder qb = YdbQueryBuilder.selectFromTable(tableName);
        qb.fields(YdbQueryBuilder.DEFAULT_FIELDS);

        filter.getPickedValue().ifPresent(value -> {
            qb.andCondition(new YdbQueryBuilder.PickedCondition(value));
        });

        qb.orderBy("execution_time asc");
        return qb;
    }

    /**
     * YdbQueryBuilder.DEFAULT_FIELDS:
     *  0  task_name
     *  1  task_instance
     *  2  task_data
     *  3  execution_time
     *  4  picked
     *  5  picked_by
     *  6  last_success
     *  7  last_failure
     *  8  consecutive_failures
     *  9  last_heartbeat
     * 10  version
     * 11  priority
     *
     * @param rs Result set
     * @param consumer Record consumer
     */
    private void consumeExecutionsOutput(ResultSetReader rs, Consumer<Execution> consumer) {
        while (rs.next()) {
            String taskName = rs.getColumn(0).getText();
            Optional<Task> task = taskResolver.resolve(taskName);
            if (!task.isPresent()) {
                LOG.warn("Failed to find implementation for task with name '{}'. "
                        + "Execution will be excluded from due. "
                        + "Either delete the execution from the database, "
                        + "or add an implementation for it. "
                        + "The scheduler may be configured to automatically "
                        + "delete unresolved tasks after a certain period of time.", taskName);
                continue;
            }

            String instanceId = rs.getColumn(1).getText();
            byte[] data = rs.getColumn(2).getBytes();
            Instant executionTime = rs.getColumn(3).getTimestamp();
            boolean picked = rs.getColumn(4).getBool();
            String pickedBy = rs.getColumn(5).getText();
            Instant lastSuccess = rs.getColumn(6).getTimestamp();
            Instant lastFailure = rs.getColumn(7).getTimestamp();
            int consecutiveFailures = 0;
            if (rs.getColumn(8).isOptionalItemPresent()) {
                consecutiveFailures = rs.getColumn(8).getInt32();
            }
            Instant lastHeartbeat = rs.getColumn(9).getTimestamp();
            long version = rs.getColumn(10).getInt64();

            Supplier dataSupplier = memoize(() -> serializer.deserialize(task.get().getDataClass(), data));
            Execution e = new Execution(executionTime,
                    new TaskInstance(taskName, instanceId, dataSupplier),
                    picked, pickedBy, lastSuccess, lastFailure,
                    consecutiveFailures, lastHeartbeat, version);
            consumer.accept(e);
        }
    }

    private void queryAndConsume(YdbQueryBuilder qb, Consumer<Execution> consumer) {
        String sql = qb.getQuery();
        Params params = qb.getParams();
        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SNAPSHOT_RO, params)))
                .join().getValue().getResultSet(0);
        consumeExecutionsOutput(result, consumer);
    }

    @Override
    public void getScheduledExecutions(
            ScheduledExecutionsFilter filter,
            Consumer<Execution> consumer) {
        YdbQueryBuilder.UnresolvedFilter unresolvedFilter
                = new YdbQueryBuilder.UnresolvedFilter(taskResolver.getUnresolved());
        YdbQueryBuilder qb = queryForFilter(filter);
        if (unresolvedFilter.isActive()) {
            qb.andCondition(unresolvedFilter);
        }
        queryAndConsume(qb, consumer);
    }

    @Override
    public void getScheduledExecutions(
            ScheduledExecutionsFilter filter, String taskName,
            Consumer<Execution> consumer) {
        YdbQueryBuilder qb = queryForFilter(filter);
        qb.andCondition(new YdbQueryBuilder.TaskCondition(taskName));
        queryAndConsume(qb, consumer);
    }

    @Override
    public List<Execution> getDue(Instant now, int limit) {
        YdbQueryBuilder qb = YdbQueryBuilder.selectFromTable(tableName);
        qb.fields(YdbQueryBuilder.DEFAULT_FIELDS)
                .andCondition(new YdbQueryBuilder.PickedCondition(false))
                .andCondition(new YdbQueryBuilder.ExecTimeCondition(now))
                .orderBy("execution_time asc");

        YdbQueryBuilder.UnresolvedFilter unresolvedFilter
                = new YdbQueryBuilder.UnresolvedFilter(taskResolver.getUnresolved());
        if (unresolvedFilter.isActive()) {
            qb.andCondition(unresolvedFilter);
        }

        List<Execution> retval = new ArrayList<>();
        queryAndConsume(qb, e -> retval.add(e));
        return retval;
    }

    @Override
    public void remove(Execution execution) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $task_name AS Text; ");
        sb.append("DECLARE $task_instance AS Text; ");
        sb.append("DECLARE $version AS Int64; ");
        sb.append("SELECT 1 AS a FROM `").append(tableName)
                .append("` WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version; ");
        sb.append("DELETE FROM `").append(tableName)
                .append("` WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version;");
        String sql = sb.toString();
        Params params = Params.of(
                "$task_name", PrimitiveValue.newText(execution.taskInstance.getTaskName()),
                "$task_instance", PrimitiveValue.newText(execution.taskInstance.getId()),
                "$version", PrimitiveValue.newInt64(execution.version)
        );
        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SERIALIZABLE_RW, params)))
                .join().getValue().getResultSet(0);
        if (! result.next()) {
            throw new RuntimeException("Expected one execution to be removed, "
                    + "but the record was not found. Indicates a bug.");
        }
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime,
            Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return rescheduleInternal(execution, nextExecutionTime, null,
                lastSuccess, lastFailure, consecutiveFailures);
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime,
            Object newData, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return rescheduleInternal(execution, nextExecutionTime,
                new NewData(newData), lastSuccess, lastFailure, consecutiveFailures);
    }

    private static Value<?> makeTimestamp(Instant instant) {
        return instant==null ?
                TYPE_TS.emptyValue() :
                PrimitiveValue.newTimestamp(instant).makeOptional();
    }

    private boolean rescheduleInternal(Execution execution, Instant nextExecutionTime,
            NewData newData, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $task_name AS Text; ")
                .append("DECLARE $task_instance AS Text; ")
                .append("DECLARE $version AS Int64; ");
        sb.append("DECLARE $picked AS Boolean; ")
                .append("DECLARE $picked_by AS Text?; ")
                .append("DECLARE $last_heartbeat AS Timestamp?; ")
                .append("DECLARE $last_success AS Timestamp?; ")
                .append("DECLARE $last_failure AS Timestamp?; ")
                .append("DECLARE $consecutive_failures AS Int32; ")
                .append("DECLARE $execution_time AS Timestamp; ");
        if (newData != null) {
            sb.append("DECLARE $task_data AS String; ");
        }
        sb.append("SELECT 1 AS a FROM `").append(tableName)
                .append("` WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version; ");
        sb.append("UPDATE `").append(tableName).append("` SET")
                .append(" picked=$picked")
                .append(",picked_by=$picked_by")
                .append(",last_heartbeat=$last_heartbeat")
                .append(",last_success=$last_success")
                .append(",last_failure=$last_failure")
                .append(",consecutive_failures=$consecutive_failures")
                .append(",execution_time=$execution_time");
        if (newData != null) {
            sb.append(",task_data=$task_data");
        }
        sb.append(",version=version + 1")
                .append(" WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version;");

        String sql = sb.toString();

        Params params = Params.create();
        params.put("$task_name", PrimitiveValue.newText(execution.taskInstance.getTaskName()));
        params.put("$task_instance", PrimitiveValue.newText(execution.taskInstance.getId()));
        params.put("$version", PrimitiveValue.newInt64(execution.version));
        params.put("$picked", PrimitiveValue.newBool(false));
        params.put("$picked_by", TYPE_TEXT.emptyValue());
        params.put("$last_heartbeat", TYPE_TS.emptyValue());
        params.put("$last_success", makeTimestamp(lastSuccess));
        params.put("$last_failure", makeTimestamp(lastFailure));
        params.put("$consecutive_failures", PrimitiveValue.newInt32(consecutiveFailures));
        params.put("$execution_time", PrimitiveValue.newTimestamp(nextExecutionTime));
        if (newData != null) {
            params.put("$task_data", PrimitiveValue.newBytes(serializer.serialize(newData.data)));
        }

        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SERIALIZABLE_RW, params)))
                .join().getValue().getResultSet(0);
        return result.next();
    }

    @Override
    public Optional<Execution> pick(Execution e, Instant timePicked) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $task_name AS Text; ")
                .append("DECLARE $task_instance AS Text; ")
                .append("DECLARE $version AS Int64; ");
        sb.append("DECLARE $picked AS Boolean; ")
                .append("DECLARE $picked_before AS Boolean; ")
                .append("DECLARE $picked_by AS Text?; ")
                .append("DECLARE $last_heartbeat AS Timestamp?; ")
                .append("DECLARE $execution_time AS Timestamp; ");
        sb.append("SELECT 1 AS a FROM `").append(tableName)
                .append("` WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version ")
                .append(" AND picked=$picked_before; ");
        sb.append("UPDATE `").append(tableName).append("` SET")
                .append(" picked=$picked")
                .append(",picked_by=$picked_by")
                .append(",last_heartbeat=$last_heartbeat")
                .append(",version=version + 1")
                .append(" WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version")
                .append(" AND picked=$picked_before;");

        String sql = sb.toString();
        Params params = Params.of(
                "$task_name", PrimitiveValue.newText(e.taskInstance.getTaskName()),
                "$task_instance", PrimitiveValue.newText(e.taskInstance.getId()),
                "$version", PrimitiveValue.newInt64(e.version),
                "$picked", PrimitiveValue.newBool(true),
                "$picked_before", PrimitiveValue.newBool(false),
                "$picked_by", PrimitiveValue.newText(schedulerSchedulerName.getName()).makeOptional(),
                "$last_heartbeat", TYPE_TS.emptyValue()
        );
        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SERIALIZABLE_RW, params)))
                .join().getValue().getResultSet(0);

        if (result.next()) {
            final Optional<Execution> pickedExecution = getExecution(e.taskInstance);
            if (!pickedExecution.isPresent()) {
                throw new IllegalStateException("Unable to find picked execution. Must have been deleted by another thread. Indicates a bug.");
            } else if (!pickedExecution.get().isPicked()) {
                throw new IllegalStateException("Picked execution does not have expected state in database: " + pickedExecution.get());
            }
            return pickedExecution;
        } else {
            LOG.trace("Failed to pick execution {}. It must have been picked by another scheduler.", e);
            return Optional.empty();
        }
    }

    @Override
    public List<Execution> getDeadExecutions(Instant olderThan) {
        YdbQueryBuilder qb = YdbQueryBuilder.selectFromTable(tableName)
                .andCondition(new YdbQueryBuilder.LastHeartbeatCondition(olderThan))
                .andCondition(new YdbQueryBuilder.PickedCondition(true))
                .orderBy("last_heartbeat asc");

        YdbQueryBuilder.UnresolvedFilter unresolvedFilter
                = new YdbQueryBuilder.UnresolvedFilter(taskResolver.getUnresolved());
        if (unresolvedFilter.isActive()) {
            qb.andCondition(unresolvedFilter);
        }

        List<Execution> retval = new ArrayList<>();
        queryAndConsume(qb, e -> retval.add(e));
        return retval;
    }

    @Override
    public void updateHeartbeat(Execution e, Instant newHeartbeat) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $task_name AS Text; ")
                .append("DECLARE $task_instance AS Text; ")
                .append("DECLARE $version AS Int64; ")
                .append("DECLARE $last_heartbeat AS Timestamp?; ");
        sb.append("SELECT 1 AS a FROM `").append(tableName)
                .append("` WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version; ");
        sb.append("UPDATE `").append(tableName).append("` SET")
                .append(" last_heartbeat=$last_heartbeat")
                .append(" WHERE task_name=$task_name")
                .append(" AND task_instance=$task_instance")
                .append(" AND version=$version;");

        String sql = sb.toString();
        Params params = Params.of(
                "$task_name", PrimitiveValue.newText(e.taskInstance.getTaskName()),
                "$task_instance", PrimitiveValue.newText(e.taskInstance.getId()),
                "$version", PrimitiveValue.newInt64(e.version),
                "$last_heartbeat", PrimitiveValue.newTimestamp(newHeartbeat).makeOptional()
        );
        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SERIALIZABLE_RW, params)))
                .join().getValue().getResultSet(0);
        if (result.next()) {
            LOG.debug("Updated heartbeat for execution: {}", e);
        } else {
            LOG.trace("Did not update heartbeat. Execution must have been removed or rescheduled: {}", e);
        }
    }

    @Override
    public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
        YdbQueryBuilder qb = YdbQueryBuilder.selectFromTable(tableName)
                .andCondition(new YdbQueryBuilder.LongFailingCondition(Instant.now().minus(interval)));

        YdbQueryBuilder.UnresolvedFilter unresolvedFilter
                = new YdbQueryBuilder.UnresolvedFilter(taskResolver.getUnresolved());
        if (unresolvedFilter.isActive()) {
            qb.andCondition(unresolvedFilter);
        }

        List<Execution> retval = new ArrayList<>();
        queryAndConsume(qb, e -> retval.add(e));
        return retval;
    }

    public Optional<Execution> getExecution(TaskInstance taskInstance) {
        return getExecution(taskInstance.getTaskName(), taskInstance.getId());
    }

    @Override
    public Optional<Execution> getExecution(String taskName, String taskInstanceId) {
        YdbQueryBuilder qb = YdbQueryBuilder.selectFromTable(tableName)
                .andCondition(new YdbQueryBuilder.TaskCondition(taskName))
                .andCondition(new YdbQueryBuilder.InstanceCondition(taskInstanceId));
        List<Execution> executions = new ArrayList<>(1);
        queryAndConsume(qb, e -> executions.add(e));
        if (executions.size() > 1) {
            throw new RuntimeException(String.format("Found more than one matching execution "
                    + "for task name/id combination: '%s'/'%s'", taskName, taskInstanceId));
        }
        return executions.size() == 1 ? Optional.ofNullable(executions.get(0)) : Optional.empty();
    }

    @Override
    public int removeExecutions(String taskName) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $task_name AS Text; ");
        sb.append("SELECT COUNT(*) AS cnt FROM `").append(tableName)
                .append("` WHERE task_name=$task_name; ");
        sb.append("DELETE FROM `").append(tableName)
                .append(" WHERE task_name=$task_name;");
        String sql = sb.toString();
        Params params = Params.of(
                "$task_name", PrimitiveValue.newText(taskName)
        );
        ResultSetReader result = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SERIALIZABLE_RW, params)))
                .join().getValue().getResultSet(0);
        if (result.next()) {
            return result.getColumn(0).getInt32();
        }
        return 0;
    }

    private static <T> Supplier<T> memoize(Supplier<T> original) {
        return new Supplier<T>() {
            Supplier<T> delegate = this::firstTime;
            boolean initialized;

            @Override
            public T get() {
                return delegate.get();
            }

            private synchronized T firstTime() {
                if (!initialized) {
                    T value = original.get();
                    delegate = () -> value;
                    initialized = true;
                }
                return delegate.get();
            }
        };
    }

    private static class NewData {
        private final Object data;
        NewData(Object data) {
            this.data = data;
        }
    }

}
