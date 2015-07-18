package org.rakam.analysis.stream.processor;

import com.facebook.presto.spi.Page;
import org.rakam.collection.SchemaField;

import java.util.Iterator;
import java.util.List;

/**
* Created by buremba <Burak Emre Kabakcı> on 14/07/15 15:59.
*/
public interface Processor {
    public void addInput(Page page);
    public Iterator<Page> getOutput();
    List<SchemaField> getSchema();
}
