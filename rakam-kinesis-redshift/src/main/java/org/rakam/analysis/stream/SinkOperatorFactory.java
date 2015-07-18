package org.rakam.analysis.stream;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
* Created by buremba <Burak Emre Kabakcı> on 14/07/15 16:25.
*/
class SinkOperatorFactory implements OperatorFactory {
    private final List<Type> sourceType;
    private final int operatorId;
    private final Consumer<Page> sink;

    public SinkOperatorFactory(List<Type> sourceType, int operatorId, Consumer<Page> sink) {
        this.sourceType = sourceType;
        this.operatorId = operatorId;
        this.sink = sink;
    }

    @Override
    public List<Type> getTypes() {
        return sourceType;
    }

    @Override
    public Operator createOperator(DriverContext driverContext) {
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, this.getClass().getSimpleName());

        return new Operator() {
            AtomicBoolean finished = new AtomicBoolean(false);

            @Override
            public OperatorContext getOperatorContext() {
                return operatorContext;
            }

            @Override
            public List<Type> getTypes() {
                return sourceType;
            }

            @Override
            public void finish() {
                finished.set(true);
            }

            @Override
            public boolean isFinished() {
                return finished.get();
            }

            @Override
            public boolean needsInput() {
                return !finished.get();
            }

            @Override
            public void addInput(Page page) {
                sink.accept(page);
            }

            @Override
            public Page getOutput() {
                return null;
            }
        };
    }

    @Override
    public void close() {

    }
}
