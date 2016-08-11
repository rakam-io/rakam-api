package org.rakam.presto.collection;

import com.amazonaws.util.Base64;
import com.google.common.collect.ImmutableMap;
import io.netty.util.CharsetUtil;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.kinesis.AWSKinesisEventStore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.CopyEvent;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public class PrestoCopyEvent implements CopyEvent
{
    private final PrestoConfig prestoConfig;
    private final PrestoQueryExecutor prestoQueryExecutor;

    @Inject
    public PrestoCopyEvent(PrestoConfig prestoConfig, PrestoQueryExecutor prestoQueryExecutor)
    {
        this.prestoConfig = prestoConfig;
        this.prestoQueryExecutor = prestoQueryExecutor;
    }

    @Override
    public QueryExecution copy(String project, String collection, List<URL> urls, EventStore.CopyType type, EventStore.CompressionType compressionType, Map<String, String> options)
    {
        if (type == null) {
            throw new RakamException("source type is missing", BAD_REQUEST);
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.put(format("%s.urls", prestoConfig.getBulkConnector()),
                Base64.encodeAsString(JsonHelper.encodeAsBytes(urls)));

        builder.put(format("%s.source_type", prestoConfig.getBulkConnector()), type.name());

        if (options != null) {
            builder.put(format("%s.source_options", prestoConfig.getBulkConnector()),
                    Base64.encodeAsString(JsonHelper.encode(options).getBytes(CharsetUtil.UTF_8)));
        }

        if (compressionType != null) {
            builder.put(format("%s.compression", prestoConfig.getBulkConnector()), type.name());
        }

        return prestoQueryExecutor.executeRawQuery(format("insert into %s.%s.%s select * from %s.%s.%s",
                prestoConfig.getColdStorageConnector(), project, ValidationUtil.checkCollection(collection),
                prestoConfig.getBulkConnector(), project, ValidationUtil.checkCollection(collection)),
                builder.build(), "middleware");
    }
}
