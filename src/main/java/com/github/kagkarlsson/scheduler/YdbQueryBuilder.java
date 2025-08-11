package com.github.kagkarlsson.scheduler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

/**
 * Custom SQL query builder for YDB.
 *
 * @author mzinal
 */
public class YdbQueryBuilder {

    public static final String DEFAULT_FIELDS
            = "task_name,task_instance,task_data,execution_time,picked,"
            + "picked_by,last_success,last_failure,consecutive_failures,"
            + "last_heartbeat,version,priority";

    private final String tableName;
    private final ArrayList<Condition> conditions = new ArrayList<>();
    private Optional<String> orderBy = Optional.empty();
    private Optional<String> fields = Optional.empty();
    private Optional<Integer> limit = Optional.empty();

    public YdbQueryBuilder(String tableName) {
        this.tableName = tableName;
    }

    public static YdbQueryBuilder selectFromTable(String tableName) {
        return new YdbQueryBuilder(tableName);
    }

    public YdbQueryBuilder andCondition(Condition c) {
        conditions.add(c);
        return this;
    }

    public YdbQueryBuilder orderBy(String orderBy) {
        this.orderBy = Optional.of(orderBy);
        return this;
    }

    public YdbQueryBuilder fields(String fields) {
        this.fields = Optional.of(fields);
        return this;
    }

    public YdbQueryBuilder limit(int limit) {
        this.limit = Optional.of(limit);
        return this;
    }

    public String getQuery() {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (Condition cond : conditions) {
            String part = cond.getParametersPart(++index);
            if (part!=null) {
                part = part.trim();
            }
            if (part!=null && part.length() > 0) {
                sb.append(part).append("; ");
            }
        }
        sb.append("SELECT ");
        if (fields.isPresent()) {
            sb.append(fields.get());
        } else {
            sb.append("*");
        }
        sb.append(" FROM ").append(tableName);

        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            index = 0;
            for (Condition cond : conditions) {
                String part = cond.getQueryPart(++index);
                if (part!=null) {
                    part = part.trim();
                }
                if (part!=null && part.length() > 0) {
                    if (index > 1) {
                        sb.append(" AND ");
                    }
                    sb.append(part);
                }
            }
        }

        orderBy.ifPresent(o -> sb.append(" ORDER BY ").append(o));
        limit.ifPresent(o -> sb.append(" LIMIT ").append(limit));
        sb.append(";");
        return sb.toString();
    }

    public Params getParams() {
        Params p = Params.create(conditions.size());
        int index = 0;
        for (Condition cond : conditions) {
            cond.setParameters(++index, p);
        }
        return p;
    }

    public static interface Condition {

        String getQueryPart(int index);

        String getParametersPart(int index);

        void setParameters(int index, Params params);

    }

    public static abstract class SingleParamCondition implements Condition {
        protected abstract String getParamType();

        @Override
        public String getParametersPart(int index) {
            return "DECLARE $p" + String.valueOf(index) + " AS " + getParamType();
        }
    }

    public static abstract class TextCondition extends SingleParamCondition {
        private final String value;

        public TextCondition(String value) {
            this.value = value;
        }

        @Override
        public String getParamType() {
            return "Text";
        }

        @Override
        public void setParameters(int index, Params params) {
            params.put("$p" + String.valueOf(index), PrimitiveValue.newText(value));
        }
    }

    public static abstract class BoolCondition extends SingleParamCondition {
        private final boolean value;

        public BoolCondition(boolean value) {
            this.value = value;
        }

        @Override
        public String getParamType() {
            return "Bool";
        }

        @Override
        public void setParameters(int index, Params params) {
            params.put("$p" + String.valueOf(index), PrimitiveValue.newBool(value));
        }
    }

    public static abstract class TimestampCondition extends SingleParamCondition {
        private final Instant value;

        public TimestampCondition(Instant value) {
            this.value = value;
        }

        @Override
        public String getParamType() {
            return "Timestamp";
        }

        @Override
        public void setParameters(int index, Params params) {
            params.put("$p" + String.valueOf(index), PrimitiveValue.newTimestamp(value));
        }
    }

    public static class UnresolvedFilter extends SingleParamCondition {
        private final ArrayList<TaskResolver.UnresolvedTask> unresolved;

        public UnresolvedFilter(Collection<TaskResolver.UnresolvedTask> unresolved) {
            this.unresolved = new ArrayList<>(unresolved);
        }

        public boolean isActive() {
            return !unresolved.isEmpty();
        }

        @Override
        public String getQueryPart(int index) {
            return "task_name NOT IN $p" + String.valueOf(index);
        }

        @Override
        public String getParamType() {
            return "List<Text>";
        }

        @Override
        public void setParameters(int index, Params params) {
            String pname = "$p" + String.valueOf(index);
            if (unresolved.isEmpty()) {
                params.put(pname, ListValue.empty(PrimitiveType.Text));
            } else {
                PrimitiveValue[] taskNames = unresolved.stream()
                        .map(TaskResolver.UnresolvedTask::getTaskName)
                        .map(PrimitiveValue::newText)
                        .toArray(PrimitiveValue[]::new);
                params.put(pname, ListValue.of(taskNames));
            }
        }
    }

    public static class PickedCondition extends BoolCondition {
        public PickedCondition(boolean value) {
            super(value);
        }

        @Override
        public String getQueryPart(int index) {
            return "picked=$p" + String.valueOf(index);
        }
    }

    public static class TaskCondition extends TextCondition {
        public TaskCondition(String value) {
            super(value);
        }

        @Override
        public String getQueryPart(int index) {
            return "task_name=$p" + String.valueOf(index);
        }
    }

    public static class InstanceCondition extends TextCondition {
        public InstanceCondition(String value) {
            super(value);
        }

        @Override
        public String getQueryPart(int index) {
            return "task_instance=$p" + String.valueOf(index);
        }
    }

    public static class ExecTimeCondition extends TimestampCondition {
        public ExecTimeCondition(Instant value) {
            super(value);
        }

        @Override
        public String getQueryPart(int index) {
            return "execution_time<=$p" + String.valueOf(index);
        }
    }

    public static class LastHeartbeatCondition extends TimestampCondition {
        public LastHeartbeatCondition(Instant value) {
            super(value);
        }

        @Override
        public String getQueryPart(int index) {
            return "last_heartbeat<=$p" + String.valueOf(index);
        }
    }

    public static class LongFailingCondition extends TimestampCondition {
        public LongFailingCondition(Instant value) {
            super(value);
        }

        @Override
        public String getQueryPart(int index) {
            return "((last_success is null and last_failure is not null)"
                    + "or (last_failure is not null and last_success < "
                    + "$p" + String.valueOf(index) + "))";
        }
    }
}
