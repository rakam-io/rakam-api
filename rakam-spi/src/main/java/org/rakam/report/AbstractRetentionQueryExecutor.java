package org.rakam.report;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.collection.SchemaField;
import org.rakam.util.RakamException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.*;

public abstract class AbstractRetentionQueryExecutor
        implements RetentionQueryExecutor {
    protected String getTimeExpression(DateUnit dateUnit) {
        if (dateUnit == DAY) {
            return "cast(%s as date)";
        }
        if (dateUnit == WEEK) {
            return "cast(date_trunc('week', %s) as date)";
        }
        if (dateUnit == MONTH) {
            return "cast(date_trunc('month', %s) as date)";
        }

        throw new UnsupportedOperationException(dateUnit + " is not supported.");
    }

    protected boolean testDeviceIdExists(Optional<RetentionAction> firstAction, Map<String, List<SchemaField>> collections) {
        if (firstAction.isPresent()) {
            List<SchemaField> schemaFields = collections.get(firstAction.get().collection());
            if (schemaFields == null) {
                throw new RakamException("The collection in first action does not exist.", HttpResponseStatus.BAD_REQUEST);
            }
            return schemaFields.stream().anyMatch(e -> e.getName().equals("_device_id"));
        } else {
            return collections.entrySet().stream()
                    .allMatch(e -> e.getValue().stream().anyMatch(field -> field.getName().equals("_device_id")));
        }
    }
}
