package com.github.kagkarlsson.scheduler.utils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author mzinal
 */
public class YdbTaskEntityDao {

    private final SessionRetryContext retryCtx;
    private final String tableName;

    public YdbTaskEntityDao(QueryClient qc, String tableName) {
        this.retryCtx = SessionRetryContext.create(qc)
                .idempotent(true).build();
        this.tableName = tableName;
    }

    public void createTable() {
        String sql = "CREATE TABLE `" + tableName + "` (\n"
                + "    task_name Text NOT NULL,\n"
                + "    task_instance Text NOT NULL,\n"
                + "    task_data String,\n"
                + "    execution_time Timestamp,\n"
                + "    picked Boolean,\n"
                + "    picked_by Text,\n"
                + "    last_success Timestamp,\n"
                + "    last_failure Timestamp,\n"
                + "    consecutive_failures Int32,\n"
                + "    last_heartbeat Timestamp,\n"
                + "    version Int64,\n"
                + "    priority Int32,\n"
                + "    PRIMARY KEY (task_name, task_instance)\n"
                + ");\n";
        retryCtx.supplyResult(qs -> qs.createQuery(sql, TxMode.NONE).execute())
                .join().getStatus().expectSuccess();
    }

    public void dropTable() {
        String sql = "DROP TABLE `" + tableName + "`;";
        retryCtx.supplyResult(qs -> qs.createQuery(sql, TxMode.NONE).execute())
                .join().getStatus().expectSuccess();
    }

    public List<YdbTaskEntity> readAll() {
        String sql = "SELECT * FROM `" + tableName + "` ORDER BY task_name, task_instance";
        ResultSetReader rs = retryCtx.supplyResult(qs -> QueryReader.readFrom(
                qs.createQuery(sql, TxMode.SNAPSHOT_RO)))
                .join().getValue().getResultSet(0);
        ArrayList<YdbTaskEntity> ret = new ArrayList<>(rs.getRowCount());
        while (rs.next()) {
            ValueReader vr;
            YdbTaskEntity te = new YdbTaskEntity();
            te.setTaskName(rs.getColumn("task_name").getText());
            te.setTaskInstance(rs.getColumn("task_instance").getText());
            te.setPicked(rs.getColumn("picked").getBool());
            te.setVersion(rs.getColumn("version").getInt64());
            vr = rs.getColumn("task_data");
            if (vr.isOptionalItemPresent()) {
                te.setTaskData(vr.getBytes());
            }
            vr = rs.getColumn("execution_time");
            if (vr.isOptionalItemPresent()) {
                te.setExecutionTime(vr.getTimestamp());
            }
            vr = rs.getColumn("picked_by");
            if (vr.isOptionalItemPresent()) {
                te.setPickedBy(vr.getText());
            }
            vr = rs.getColumn("last_success");
            if (vr.isOptionalItemPresent()) {
                te.setLastSuccess(vr.getTimestamp());
            }
            vr = rs.getColumn("last_failure");
            if (vr.isOptionalItemPresent()) {
                te.setLastFailure(vr.getTimestamp());
            }
            vr = rs.getColumn("consecutive_failures");
            if (vr.isOptionalItemPresent()) {
                te.setConsecutiveFailures(vr.getInt32());
            }
            vr = rs.getColumn("last_heartbeat");
            if (vr.isOptionalItemPresent()) {
                te.setLastHeartbeat(vr.getTimestamp());
            }
            ret.add(te);
        }
        return ret;
    }

    public void save(YdbTaskEntity... tes) {
        save(Arrays.asList(tes));
    }

    public void save(Collection<YdbTaskEntity> tes) {
        tes.stream().map(te -> mapEntity(te));
    }

    private StructValue mapEntity(YdbTaskEntity te) {
        final HashMap<String, Value<?>> m = new HashMap();
        m.put("task_name", PrimitiveValue.newText(te.getTaskName()));
        m.put("task_instance", PrimitiveValue.newText(te.getTaskInstance()));
        m.put("task_data", makeBytes(te.getTaskData()));
        m.put("execution_time", makeTimestamp(te.getExecutionTime()));
        m.put("picked", PrimitiveValue.newBool(te.isPicked()));
        m.put("picked_by", makeText(te.getPickedBy()));
        m.put("last_success", makeTimestamp(te.getLastSuccess()));
        m.put("last_failure", makeTimestamp(te.getLastFailure()));
        m.put("consecutive_failures", makeInt32(te.getConsecutiveFailures()));
        m.put("last_heartbeat", makeTimestamp(te.getLastHeartbeat()));
        m.put("version", PrimitiveValue.newInt64(te.getVersion()));
        m.put("priority", PrimitiveValue.newInt32(0));
        return StructValue.of(m);
    }

    private OptionalValue makeText(String in) {
        if (in == null) {
            return PrimitiveType.Text.makeOptional().emptyValue();
        }
        return PrimitiveValue.newText(in).makeOptional();
    }

    private OptionalValue makeBytes(byte[] in) {
        if (in == null) {
            return PrimitiveType.Bytes.makeOptional().emptyValue();
        }
        return PrimitiveValue.newBytes(in).makeOptional();
    }

    private OptionalValue makeTimestamp(Instant in) {
        if (in == null) {
            return PrimitiveType.Timestamp.makeOptional().emptyValue();
        }
        return PrimitiveValue.newTimestamp(in).makeOptional();
    }

    private OptionalValue makeInt32(int in) {
        if (in <= 0) {
            return PrimitiveType.Int32.makeOptional().emptyValue();
        }
        return PrimitiveValue.newInt32(in).makeOptional();
    }
}
