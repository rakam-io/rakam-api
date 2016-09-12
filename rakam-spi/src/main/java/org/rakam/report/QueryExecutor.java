package org.rakam.report;


import com.facebook.presto.sql.tree.QualifiedName;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Optional;

public interface QueryExecutor {
    QueryExecution executeRawQuery(String sqlQuery);
    QueryExecution executeRawStatement(String sqlQuery);
    String formatTableReference(String project, QualifiedName name, Optional<Sample> sample);

    class Sample {
        public final SampleMethod method;
        public final int percentage;

        @JsonCreator
        public Sample(SampleMethod method, int percentage) {
            this.method = method;
            this.percentage = percentage;
        }

        public enum SampleMethod {
            BERNOULLI, SYSTEM
        }
    }
}
