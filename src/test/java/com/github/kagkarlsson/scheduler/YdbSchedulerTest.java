package com.github.kagkarlsson.scheduler;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import tech.ydb.query.QueryClient;

import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbSchedulerTest {

    private static final Logger LOG = LoggerFactory.getLogger(YdbSchedulerTest.class);

    @RegisterExtension
    public static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static String connectionString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ydb.useTls() ? "grpcs://" : "grpc://" );
        sb.append(ydb.endpoint());
        sb.append(ydb.database());
        return sb.toString();
    }

    @Test
    void schedulerOneTimeTest() {
        try (GrpcTransport gt = GrpcTransport.forConnectionString(connectionString())
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build()) {
            try (QueryClient qc = QueryClient.newClient(gt).build()) {
                SchedulerBuilder builder = YdbScheduler.create(qc, YdbTaskRepository.DEFAULT_TABLE_NAME);

                AtomicInteger count = new AtomicInteger();

                OneTimeTask<Object> task = Tasks
                    .oneTime("task", Object.class).execute((TaskInstance<Object> inst,
                        ExecutionContext ctx) -> {
                        LOG.info("Execution");
                        count.addAndGet(1);
                    });

                builder.knownTasks.add(task);
                builder.executorService(MoreExecutors.newDirectExecutorService());

                Scheduler scheduler = builder.build();
                scheduler.schedule(task.instance("instance-1"), Instant.now());

                scheduler.executeDue();

                Assertions.assertThat(count).hasValue(1);
            }
        }
    }
}
