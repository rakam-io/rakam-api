package org.rakam.collection.event.datastore;

import org.rakam.collection.event.EventStore;
import org.rakam.model.Event;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 16:22.
 */
public class PostgresqlEventStore implements EventStore {
    public PostgresqlEventStore(PostgresqlConfig postgresqlConfig) {

    }

    @Override
    public void store(Event event) {

    }

    @PreDestroy
    public void stopServer() {
        System.out.println(1);
    }

    @PostConstruct
    public void startServer() {
        System.out.println(1);
    }
}
