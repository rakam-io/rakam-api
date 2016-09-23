package org.rakam.plugin;

import org.rakam.report.QueryExecution;

import java.net.URL;
import java.util.List;
import java.util.Map;

public interface CopyEvent
{
    //use_header column_separator
    QueryExecution copy(String project, String collection, List<URL> urls, EventStore.CopyType type, EventStore.CompressionType compressionType, Map<String, String> options);
}
