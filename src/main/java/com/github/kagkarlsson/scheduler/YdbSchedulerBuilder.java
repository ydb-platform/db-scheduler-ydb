package com.github.kagkarlsson.scheduler;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.kagkarlsson.scheduler.task.Task;

import tech.ydb.query.QueryClient;

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;
import static com.github.kagkarlsson.scheduler.Scheduler.THREAD_PREFIX;

public class YdbSchedulerBuilder extends SchedulerBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(YdbSchedulerBuilder.class);

    private final QueryClient ydbClient;

    /**
     * Scheduler builder for YDB
     *
     * Example using the table 'my_scheduler_tasks':
     * SchedulerBuilder builder = new YdbSchedulerBuilder(ydbClient, knownTasks)
     *        .tableName("scheduler-collection");
     *
     * @param ydbClient - object handling ydb connection
     * @param knownTasks - list of known tasks
     */
    public YdbSchedulerBuilder(QueryClient ydbClient, List<Task<?>> knownTasks) {
        super(null, knownTasks);
        this.ydbClient = ydbClient;
    }

    @Override
    public Scheduler build() {
        if (pollingLimit < executorThreads) {
            LOG.warn("Polling-limit is less than number of threads. Should be equal or higher.");
        }

        if (schedulerName == null) {
            schedulerName = new SchedulerName.Hostname();
        }

        final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);

        final YdbTaskRepository schedulerTaskRepository = new YdbTaskRepository(taskResolver,
            schedulerName, serializer, tableName, ydbClient);
        final YdbTaskRepository clientTaskRepository = new YdbTaskRepository(taskResolver,
            schedulerName, serializer, tableName, ydbClient);

        ExecutorService candidateExecutorService = executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService = Executors
                .newFixedThreadPool(executorThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
        }

        LOG.info("Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s enable-immediate-execution={}, table-name={}, name={}",
            executorThreads,
            waiter.getWaitDuration().getSeconds(),
            heartbeatInterval.getSeconds(),
            enableImmediateExecution,
            tableName,
            schedulerName.getName());

        return new YdbScheduler(clock, schedulerTaskRepository, clientTaskRepository,
                taskResolver, executorThreads, candidateExecutorService, schedulerName, waiter,
                heartbeatInterval, enableImmediateExecution, statsRegistry,
                pollingLimit, deleteUnresolvedAfter, shutdownMaxWait,
                logLevel, logStackTrace, startTasks);
    }
}
