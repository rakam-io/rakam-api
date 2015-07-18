package org.rakam.analysis.stream;

import com.facebook.presto.Session;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
* Created by buremba <Burak Emre Kabakcı> on 13/07/15 14:37.
*/
public class MaterializedOutputFactory
        implements OutputFactory
{
    private final AtomicReference<MaterializingOperator> materializingOperator = new AtomicReference<>();
    private final Session session;

    public MaterializedOutputFactory(Session session) {
        this.session = session;
    }

    public MaterializingOperator getMaterializingOperator()
    {
        MaterializingOperator operator = materializingOperator.get();
        checkState(operator != null, "Output not created");
        return operator;
    }

    @Override
    public OperatorFactory createOutputOperator(int operatorId, List<Type> sourceType)
    {
        checkNotNull(sourceType, "sourceType is null");

        return new OperatorFactory()
        {
            @Override
            public List<Type> getTypes()
            {
                return ImmutableList.of();
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MaterializingOperator.class.getSimpleName());
                MaterializingOperator operator = new MaterializingOperator(operatorContext, sourceType, session);

                if (!materializingOperator.compareAndSet(null, operator)) {
                    throw new IllegalArgumentException("Output already created");
                }
                return operator;
            }

            @Override
            public void close()
            {
            }
        };
    }
}
