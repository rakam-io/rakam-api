package org.rakam.util.javascript;

import io.airlift.log.Level;

import java.time.Instant;

public interface JSLoggerService {

    ILogger createLogger(String project, String prefix);

    ILogger createLogger(String project, String prefix, String identifier);

    class LogEntry {
        public final String id;
        public final Level level;
        public final String message;
        public final Instant timestamp;

        public LogEntry(String id, Level level, String message, Instant timestamp) {
            this.id = id;
            this.level = level;
            this.message = message;
            this.timestamp = timestamp;
        }
    }
}
