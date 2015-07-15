package org.rakam.analysis;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.rakam.report.QueryResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.rakam.analysis.stream.UnmodifiableCollectionsSerializer.registerSerializers;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/07/15 11:07.
 */
public class QueryResultSerializer implements StreamSerializer<QueryResult> {

    private final KryoPool pool;

    public QueryResultSerializer() {
        KryoFactory factory = () -> {
            Kryo kryo = new Kryo();
            kryo.register(QueryResult.class);
            registerSerializers(kryo);
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            return kryo;
        };
        pool = new KryoPool.Builder(factory).softReferences().build();
    }

    @Override
    public void write(ObjectDataOutput objectDataOutput, QueryResult materializedRow) throws IOException {
        Kryo borrow = pool.borrow();
        Output output = new Output(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                objectDataOutput.write(b);
            }
        });
        borrow.writeObject(output, materializedRow);
        output.flush();
        pool.release(borrow);
    }

    @Override
    public QueryResult read(ObjectDataInput objectDataInput) throws IOException {
        Kryo borrow = pool.borrow();
        QueryResult materializedRow = borrow.readObject(new Input(new InputStream() {
            @Override
            public int read() throws IOException {
                return objectDataInput.readByte();
            }
        }), QueryResult.class);
        pool.release(borrow);
        return materializedRow;
    }

    @Override
    public int getTypeId() {
        return 15;
    }

    @Override
    public void destroy() {

    }
}
