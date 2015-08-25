package org.rakam.analysis.stream;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 12/07/15 08:41.
 */
public class AbstractContinuousQueryExecutor {
    private final List<Operator> operators;

    public AbstractContinuousQueryExecutor(List<Driver> drivers) {
        Driver driver = drivers.get(0);
        Field f;
        try {
            f = driver.getClass().getDeclaredField("operators");
            f.setAccessible(true);
            this.operators = (List<Operator>) f.get(driver);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    public void execute() {
        ConnectorSplit connectorSplit = new DummyConnectorSplit();
        ((SourceOperator) operators.get(0)).addSplit(new Split("stream", connectorSplit));
        try {
            for (int i = 0; i < operators.size() - 1; i++) {
                // check if current operator is blocked
                Operator current = operators.get(i);
                ListenableFuture<?> blocked = current.isBlocked();
                if (!blocked.isDone()) {
                    current.getOperatorContext().recordBlocked(blocked);
                    return;
                }

                // check if next operator is blocked
                Operator next = operators.get(i + 1);
                blocked = next.isBlocked();
                if (!blocked.isDone()) {
                    next.getOperatorContext().recordBlocked(blocked);
                    return;
                }

                // if the current operator is not finished and next operator needs input...
                    if (!current.isFinished() && next.needsInput()) {
                    // get an output page from current operator
                    current.getOperatorContext().startIntervalTimer();
                    Page page = current.getOutput();
                    current.getOperatorContext().recordGetOutput(page);

                    // if we got an output page, add it to the next operator
                    if (page != null) {
                        next.getOperatorContext().startIntervalTimer();
                        next.addInput(page);
                        next.getOperatorContext().recordAddInput(page);
                    }
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.getOperatorContext().startIntervalTimer();
                    next.finish();
                    next.getOperatorContext().recordFinish();
                }
            }
        }
        catch (Throwable t) {
            throw t;
        }
    }

    private static class DummyConnectorSplit implements ConnectorSplit {
        @Override
        public boolean isRemotelyAccessible() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HostAddress> getAddresses() {

            throw new UnsupportedOperationException();
        }

        @Override
        public Object getInfo() {
            return null;
        }
    }
}
