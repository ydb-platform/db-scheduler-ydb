package com.github.kagkarlsson.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
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
        sb.append(";");
        return sb.toString();
    }

    public static interface Condition {

        String getQueryPart(int index);

        String getParametersPart(int index);

        void setParameters(int index, Map<String, Value<?>> params);

    }

    public static class UnresolvedFilter implements Condition {
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
        public String getParametersPart(int index) {
            return "DECLARE $p" + String.valueOf(index) + " AS List<Text>";
        }

        @Override
        public void setParameters(int index, Map<String, Value<?>> params) {
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

    public static class PickedCondition implements Condition {
        private final boolean value;

        public PickedCondition(boolean value) {
            this.value = value;
        }

        @Override
        public String getQueryPart(int index) {
            return "picked=$p" + String.valueOf(index);
        }

        @Override
        public String getParametersPart(int index) {
            return "DECLARE $p" + String.valueOf(index) + "AS Boolean";
        }

        @Override
        public void setParameters(int index, Map<String, Value<?>> params) {
            params.put("$p" + String.valueOf(index), PrimitiveValue.newBool(value));
        }
    }

    public static class TaskCondition implements Condition {
        private final String value;

        public TaskCondition(String value) {
            this.value = value;
        }

        @Override
        public String getQueryPart(int index) {
            return "task_name=$p" + String.valueOf(index);
        }

        @Override
        public String getParametersPart(int index) {
            return "DECLARE $p" + String.valueOf(index) + "AS Text";
        }

        @Override
        public void setParameters(int index, Map<String, Value<?>> params) {
            params.put("$p" + String.valueOf(index), PrimitiveValue.newText(value));
        }
    }
}
