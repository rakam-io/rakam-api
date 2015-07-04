package org.rakam.analysis;

import io.airlift.configuration.Config;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/07/15 07:36.
 */
public class AWSRedshiftClusterConfig {
    private String database;
    private String identifier;
    private String nodeType = "dw.hs1.xlarge";
    private int numberOfNodes = 2;

    @Config("aws.redshift.cluster.database")
    public AWSRedshiftClusterConfig setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    @Config("aws.redshift.cluster.identifier")
    public AWSRedshiftClusterConfig setIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public String getIdentifier() {
        return identifier;
    }

    @Config("aws.redshift.cluster.node_type")
    public AWSRedshiftClusterConfig setNodeType(String nodeType) {
        this.nodeType = nodeType;
        return this;
    }

    public String getNodeType() {
        return nodeType;
    }

    @Config("aws.redshift.cluster.number_of_nodes")
    public AWSRedshiftClusterConfig setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        return this;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }
}
