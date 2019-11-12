package org.rakam.presto.analysis;

import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface SchemaDao {
    @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
            "        table_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "        schema_name VARCHAR(255) NOT NULL,\n" +
            "        table_name VARCHAR(255) NOT NULL,\n" +
            "        create_time BIGINT NOT NULL,\n" +
            "        table_version BIGINT NOT NULL,\n" +
            "        UNIQUE (schema_name, table_name)\n" +
            ")")
    void createTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS columns (\n" +
            "  table_id    BIGINT       NOT NULL,\n" +
            "  column_id   BIGINT       NOT NULL,\n" +
            "  column_name VARCHAR(255) NOT NULL,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  data_type   VARCHAR(255) NOT NULL,\n" +
            "  PRIMARY KEY (table_id, column_id),\n" +
            "  UNIQUE (table_id, column_name),\n" +
            "  FOREIGN KEY (table_id) REFERENCES tables (table_id)\n" +
            ")")
    void createColumn();


}
