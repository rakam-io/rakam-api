package org.rakam.analysis.stream;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.util.Types.checkType;

/**
* Created by buremba <Burak Emre Kabakcı> on 07/07/15 07:22.
*/
public class RakamMetadata extends ReadOnlyConnectorMetadata {
    QueryAnalyzer.QueryContext context;

    public RakamMetadata() {
    }

    public List<SchemaField> getColumns() {
        if(context == null) {
            throw new IllegalStateException();
        }
        return context.getColumns();
    }

    public void setContext(QueryAnalyzer.QueryContext context) {
        this.context = context;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.of("default");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return new RakamConnectorTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table) {
        RakamConnectorTableHandle tableHandle = checkType(table, RakamConnectorTableHandle.class, "tableHandle");

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        for (int i = 0; i < getColumns().size(); i++) {
            SchemaField column = getColumns().get(i);
            builder.add(new ColumnMetadata(column.getName(), convertColumn(column.getType()), i, false));
        }
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), builder.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        return null;
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle) {
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle) {
        return getColumns().stream().map(column -> new MyConnectorColumnHandle(column.getName(), convertColumn(column.getType())))
                .collect(Collectors.toMap(x -> x.getName(), x -> x));
    }

    public static Type convertColumn(FieldType type) {
        switch (type) {
            case STRING:
                return VarcharType.VARCHAR;
            case LONG:
                return BigintType.BIGINT;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case DATE:
                return DateType.DATE;
            case TIME:
                return TimeType.TIME;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case HYPERLOGLOG:
                return HyperLogLogType.HYPER_LOG_LOG;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static FieldType convertColumn(Type type) {
        if(type == VarcharType.VARCHAR) {
            return FieldType.STRING;
        }
        if(type == BigintType.BIGINT) {
            return FieldType.LONG;
        }
        if(type == DoubleType.DOUBLE) {
            return FieldType.DOUBLE;
        }
        if(type == BooleanType.BOOLEAN) {
            return FieldType.BOOLEAN;
        }
        if(type == DateType.DATE) {
            return FieldType.DATE;
        }
        if(type == TimeType.TIME) {
            return FieldType.TIME;
        }
        if(type == TimestampType.TIMESTAMP) {
            return FieldType.TIMESTAMP;
        }
        if(type == HyperLogLogType.HYPER_LOG_LOG) {
            return FieldType.HYPERLOGLOG;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle) {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return null;
    }

    public static class MyConnectorColumnHandle implements ConnectorColumnHandle {
        private final String name;
        private final Type type;

        public MyConnectorColumnHandle(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public Type getType() {
            return type;
        }
    }
}
