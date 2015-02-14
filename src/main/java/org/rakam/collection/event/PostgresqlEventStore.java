package org.rakam.collection.event;

import org.rakam.model.Event;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 16:22.
 */
public class PostgresqlEventStore implements EventStore {
    public PostgresqlEventStore(PostgresqlConfig postgresqlConfig) {

    }

    @Override
    public void store(Event event) {

    }
}
