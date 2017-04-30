package org.rakam.presto.analysis;

import org.rakam.analysis.AbstractFunnelQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryExecutor;

import javax.inject.Inject;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public class FastPrestoFunnelQueryExecutor extends AbstractFunnelQueryExecutor
{
    @Inject
    public FastPrestoFunnelQueryExecutor(ProjectConfig projectConfig, Metastore metastore, QueryExecutor executor)
    {
        super(projectConfig, metastore, executor);
    }

    @Override
    public String getTemplate(List<FunnelStep> steps, Optional<String> dimension, Optional<FunnelWindow> window)
    {
        return null;
    }

    @Override
    public String convertFunnel(String project, String connectorField, int idx, FunnelStep funnelStep, Optional<String> dimension, LocalDate startDate, LocalDate endDate)
    {
        return null;
    }
}
