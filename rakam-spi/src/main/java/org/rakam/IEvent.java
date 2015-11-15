package org.rakam;

import org.apache.avro.generic.GenericRecord;

public interface IEvent {
    String project();
    String collection();
    GenericRecord properties();
}
