package org.rakam;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.RecordSink;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.Types.checkType;

public class TestingConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "testing";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config) {
        return new Connector() {
            @Override
            public ConnectorHandleResolver getHandleResolver() {
                return new TestingHandleResolver();
            }

            @Override
            public ConnectorMetadata getMetadata() {
                return new TestingMetadata();
            }

            @Override
            public ConnectorSplitManager getSplitManager() {
                return new ConnectorSplitManager() {
                    @Override
                    public ConnectorSplitSource getSplits(ConnectorSession session, ConnectorTableLayoutHandle layout) {
                        TestingMetadata.InMemoryTableHandle table = checkType(layout, TestingTableLayoutHandle.class, "").getTable();
                        return new FixedSplitSource("testing", ImmutableList.of(new MockSplit(table.getTableName())));
                    }
                };
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider() {
                return new ConnectorRecordSetProvider() {
                    @Override
                    public RecordSet getRecordSet(ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
                        return null;
//                            return new InMemoryRecordSet();
                    }
                };
            }

            @Override
            public ConnectorRecordSinkProvider getRecordSinkProvider() {
                return new ConnectorRecordSinkProvider() {
                    @Override
                    public RecordSink getRecordSink(ConnectorSession session, ConnectorOutputTableHandle tableHandle) {
                        return null;
                    }

                    @Override
                    public RecordSink getRecordSink(ConnectorSession session, ConnectorInsertTableHandle tableHandle) {
                        return null;
                    }
                };
            }
        };


    }
}
