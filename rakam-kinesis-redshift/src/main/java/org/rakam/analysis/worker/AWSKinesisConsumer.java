package org.rakam.analysis.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.model.Record;
import org.rakam.analysis.S3ManifestEmitter;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 07:03.
 */
public class AWSKinesisConsumer implements IRecordProcessor {
    private final KinesisMessageEventTransformer transformer;
    private BasicMemoryBuffer<Record> buffer;
    final static Logger LOGGER = LoggerFactory.getLogger(AWSKinesisConsumer.class);
    private final S3ManifestEmitter emitter;

    public AWSKinesisConsumer(KinesisConnectorConfiguration config, Metastore metastore) {
        this.buffer = new BasicMemoryBuffer<>(config);
        this.emitter = new S3ManifestEmitter(config);
        this.transformer = new KinesisMessageEventTransformer(metastore);
    }

    @Override
    public void initialize(String shardId) {

    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            buffer.consumeRecord(record, record.getData().array().length, record.getSequenceNumber());
        }

        if (buffer.shouldFlush()) {
            List<Event> emitItems = new ArrayList<>(buffer.getRecords().size());
            try {
                for (Record record : buffer.getRecords()) {
                    emitItems.add(transformer.fromClass(record));
                }
            } catch (IOException e) {
                LOGGER.error("Error while deserializing items in Kinesis stream", e);
                return;
            } finally {
                transformer.clearSchemaCache();
            }
            emit(checkpointer, emitItems);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {

    }

    private void emit(IRecordProcessorCheckpointer checkpointer, List<Event> emitItems) {
        List<Event> unprocessed = new ArrayList<>(emitItems);
        try {
            for (int numTries = 0; numTries < 3; numTries++) {
                unprocessed = emitter.emit(new UnmodifiableBuffer<>(buffer, unprocessed));
                if (unprocessed.isEmpty()) {
                    break;
                }
            }
            if (!unprocessed.isEmpty()) {
                emitter.fail(unprocessed);
            }
            buffer.clear();
            // checkpoint once all the records have been consumed
            checkpointer.checkpoint();
        } catch (IOException | KinesisClientLibDependencyException | InvalidStateException | ThrottlingException
                | ShutdownException e) {
            LOGGER.error("Error processing Kinesis stream", e);
            emitter.fail(unprocessed);
        }
    }

}
