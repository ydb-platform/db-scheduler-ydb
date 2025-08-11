package com.github.kagkarlsson.scheduler.utils;

import java.time.Instant;

public class YdbTaskEntity {

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

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskInstance() {
        return taskInstance;
    }

    public void setTaskInstance(String taskInstance) {
        this.taskInstance = taskInstance;
    }

    public byte[] getTaskData() {
        return taskData;
    }

    public void setTaskData(byte[] taskData) {
        this.taskData = taskData;
    }

    public Instant getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(Instant executionTime) {
        this.executionTime = executionTime;
    }

    public boolean isPicked() {
        return picked;
    }

    public void setPicked(boolean picked) {
        this.picked = picked;
    }

    public String getPickedBy() {
        return pickedBy;
    }

    public void setPickedBy(String pickedBy) {
        this.pickedBy = pickedBy;
    }

    public Instant getLastSuccess() {
        return lastSuccess;
    }

    public void setLastSuccess(Instant lastSuccess) {
        this.lastSuccess = lastSuccess;
    }

    public Instant getLastFailure() {
        return lastFailure;
    }

    public void setLastFailure(Instant lastFailure) {
        this.lastFailure = lastFailure;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public void setConsecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
    }

    public Instant getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}
