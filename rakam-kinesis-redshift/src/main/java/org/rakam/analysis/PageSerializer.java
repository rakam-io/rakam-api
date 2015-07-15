package org.rakam.analysis;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.PagesSerde;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.FixedWidthBlockEncoding;
import com.facebook.presto.spi.block.LazySliceArrayBlockEncoding;
import com.facebook.presto.spi.block.SliceArrayBlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/07/15 11:05.
 */
public class PageSerializer implements StreamSerializer<Page> {

    private final BlockEncodingManager serde;

    public PageSerializer(Metadata metadata) {
        Set<BlockEncodingFactory<?>> blockEncodingFactories = Sets.newHashSet(
                VariableWidthBlockEncoding.FACTORY,
                FixedWidthBlockEncoding.FACTORY,
                SliceArrayBlockEncoding.FACTORY,
                LazySliceArrayBlockEncoding.FACTORY);

        this.serde = new BlockEncodingManager(metadata.getTypeManager(), blockEncodingFactories);
    }

    @Override
    public void write(ObjectDataOutput objectDataOutput, Page page) throws IOException {
        PagesSerde.writePages(serde, new OutputStreamSliceOutput(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                objectDataOutput.write(b);
            }
        }), page);
    }

    @Override
    public Page read(ObjectDataInput objectDataInput) throws IOException {
        return PagesSerde.readPages(serde, new InputStreamSliceInput(new InputStream() {
            @Override
            public int read() throws IOException {
                return objectDataInput.readByte();
            }
        })).next();
    }

    @Override
    public int getTypeId() {
        return 16;
    }

    @Override
    public void destroy() {

    }
}
