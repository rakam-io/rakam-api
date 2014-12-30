package org.rakam.stream.kume.service.hyperloglog;

/**
 * Created by buremba <Burak Emre Kabakcı> on 29/12/14 04:17.
 */
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;

import java.nio.charset.Charset;
import java.util.Collection;

public class HLLWrapper implements KryoSerializable {
    private HLL hll;
    private static final HashFunction hashFunction = Hashing.murmur3_128();

    public HLLWrapper() {
        hll = new HLL(13/*log2m*/, 5/*registerWidth*/);
    }

    public HLLWrapper(byte[] bytes) {
        hll = HLL.fromBytes(bytes);
    }

    public long cardinality() {
        return hll.cardinality();
    }

    public void union(HLLWrapper hll) {
        this.hll.union(hll.hll);
    }

    public void addAll(Collection<String> coll) {
        for (String a : coll) {
            hll.addRaw(hashFunction.hashString(a, Charset.forName("UTF-8")).asLong());
        }
    }

    public void add(String obj) {
        if (obj == null)
            throw new IllegalArgumentException();

        hll.addRaw(hashFunction.hashString(obj, Charset.forName("UTF-8")).asLong());
    }

    public void reset() {
        hll = new HLL(13/*log2m*/, 5/*registerWidth*/);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        byte[] bytes = hll.toBytes();
        output.writeInt(bytes.length);
        output.write(hll.toBytes());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int size = input.readInt();
        hll = HLL.fromBytes(input.readBytes(size));
    }
}
