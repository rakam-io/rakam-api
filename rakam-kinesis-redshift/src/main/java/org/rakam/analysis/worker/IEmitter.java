package org.rakam.analysis.worker;

import java.io.IOException;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 07/07/15 05:53.
 */
public interface IEmitter<T> {
    List<T> emit(List<T> buffer) throws IOException;

    /**
     * This method defines how to handle a set of records that cannot successfully be emitted.
     *
     * @param records
     *        a list of records that were not successfully emitted
     */
    void fail(List<T> records);

    /**
     * This method is called when the KinesisConnectorRecordProcessor is shutdown. It should close
     * any existing client connections.
     */
    void shutdown();
}
