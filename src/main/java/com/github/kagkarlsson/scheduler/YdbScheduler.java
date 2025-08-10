package com.github.kagkarlsson.scheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;

import tech.ydb.query.QueryClient;

public class YdbScheduler extends Scheduler {

    protected YdbScheduler(Clock clock, TaskRepository schedulerTaskRepository,
            TaskRepository clientTaskRepository, TaskResolver taskResolver,
            int threadpoolSize, ExecutorService executorService,
            SchedulerName schedulerName, Waiter executeDueWaiter,
            Duration heartbeatInterval, boolean enableImmediateExecution,
            StatsRegistry statsRegistry, int pollingLimit,
            Duration deleteUnresolvedAfter, Duration shutdownMaxWait,
            LogLevel logLevel, boolean logStackTrace, List<OnStartup> onStartup) {
        super(clock, schedulerTaskRepository, clientTaskRepository, taskResolver,
                threadpoolSize, executorService, schedulerName,
                executeDueWaiter, heartbeatInterval, enableImmediateExecution,
                statsRegistry, pollingLimit, deleteUnresolvedAfter,
                shutdownMaxWait, logLevel, logStackTrace, onStartup);
    }

    public static SchedulerBuilder create(QueryClient ydbClient, String tableName, Task<?>... knownTasks) {
        List<Task<?>> knownTasksList = new ArrayList<>();
        knownTasksList.addAll(Arrays.asList(knownTasks));
        return create(ydbClient, tableName, knownTasksList);
    }

    public static SchedulerBuilder create(QueryClient ydbClient, String tableName, List<Task<?>> knownTasks) {
        return new YdbSchedulerBuilder(ydbClient, knownTasks).tableName(tableName);
    }
}
