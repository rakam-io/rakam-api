package org.rakam.analysis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 07:03.
 */
public class AWSKinesisConsumer implements IRecordProcessor {
    private Logger logger;

    @Override
    public void initialize(String shardId) {

    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            record.getData().array();
            System.out.println(record);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {

    }
}
