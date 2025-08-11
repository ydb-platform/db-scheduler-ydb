package com.github.kagkarlsson.scheduler.utils;

import java.time.Instant;

public final class YdbTaskEntityBuilder {

    private String taskName;
    private String taskInstance;
    private byte[] taskData;
    private Instant executionTime;
    private boolean picked;
    private String pickedBy;
    private Instant lastSuccess;
    private Instant lastFailure;
    private int consecutiveFailures;
    private Instant lastHeartbeat;
    private long version;

    private YdbTaskEntityBuilder() {
    }

    public static YdbTaskEntityBuilder aTaskEntity() {
        return new YdbTaskEntityBuilder();
    }

    public YdbTaskEntityBuilder taskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    public YdbTaskEntityBuilder taskInstance(String taskInstance) {
        this.taskInstance = taskInstance;
        return this;
    }

    public YdbTaskEntityBuilder taskData(byte[] taskData) {
        this.taskData = taskData;
        return this;
    }

    public YdbTaskEntityBuilder executionTime(Instant executionTime) {
        this.executionTime = executionTime;
        return this;
    }

    public YdbTaskEntityBuilder picked(boolean picked) {
        this.picked = picked;
        return this;
    }

    public YdbTaskEntityBuilder pickedBy(String pickedBy) {
        this.pickedBy = pickedBy;
        return this;
    }

    public YdbTaskEntityBuilder lastSuccess(Instant lastSuccess) {
        this.lastSuccess = lastSuccess;
        return this;
    }

    public YdbTaskEntityBuilder lastFailure(Instant lastFailure) {
        this.lastFailure = lastFailure;
        return this;
    }

    public YdbTaskEntityBuilder consecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
        return this;
    }

    public YdbTaskEntityBuilder lastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
        return this;
    }

    public YdbTaskEntityBuilder version(long version) {
        this.version = version;
        return this;
    }

    public YdbTaskEntity build() {
        YdbTaskEntity taskEntity = new YdbTaskEntity();
        taskEntity.setTaskName(taskName);
        taskEntity.setTaskInstance(taskInstance);
        taskEntity.setTaskData(taskData);
        taskEntity.setExecutionTime(executionTime);
        taskEntity.setPicked(picked);
        taskEntity.setPickedBy(pickedBy);
        taskEntity.setLastSuccess(lastSuccess);
        taskEntity.setLastFailure(lastFailure);
        taskEntity.setConsecutiveFailures(consecutiveFailures);
        taskEntity.setLastHeartbeat(lastHeartbeat);
        taskEntity.setVersion(version);
        return taskEntity;
    }
}
