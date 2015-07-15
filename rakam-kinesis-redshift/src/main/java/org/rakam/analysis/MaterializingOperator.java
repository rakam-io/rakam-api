package org.rakam.analysis;

import com.facebook.presto.Session;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
* Created by buremba <Burak Emre Kabakcı> on 13/07/15 13:52.
*/
public class MaterializingOperator
        implements Operator
{
    private final List<Type> types;
    private final ConnectorSession session;
    private final OperatorContext operatorContext;
    private final List<List<Object>> result;
    private boolean finished;
    private boolean closed;

    public MaterializingOperator(OperatorContext operatorContext, List<Type> sourceTypes, Session session)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.result = new ArrayList<>();
        this.types = sourceTypes;
        this.session = session.toConnectorSession();
    }

    public boolean isClosed()
    {
        return closed;
    }

    public List<List<Object>> getMaterializedResult()
    {
        return result;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finished, "operator finished");

        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(page.getChannelCount());
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = types.get(channel);
                Block block = page.getBlock(channel);
                values.add(type.getObjectValue(session, block, position));
            }
            values = Collections.unmodifiableList(values);

            result.add(values);
        }

        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
