package org.rakam;

import org.rakam.analysis.TestMaterializedView;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public final class PGClock extends TestMaterializedView.IncrementableClock implements Serializable {

    private PostgresqlQueryExecutor queryExecutor;

    public PGClock(PostgresqlQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void increment(long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long millis() {
        return instant().toEpochMilli();
    }

    @Override
    public Instant instant() {
        return queryExecutor.runRawQuery("select now()", resultSet -> {
            resultSet.next();
            return resultSet.getTimestamp(1).toInstant();
        });
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PGClock) {
            PGClock other = (PGClock) obj;
            return instant().equals(other.instant());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return instant().hashCode();
    }

    @Override
    public String toString() {
        return "PGClock[" + instant() + "]";
    }
}
