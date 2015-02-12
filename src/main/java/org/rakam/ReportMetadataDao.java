package org.rakam;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/02/15 02:22.
 */
public interface ReportMetadataDao {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS reports (\n" +
                "  id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
                "  project VARCHAR(255) NOT NULL,\n" +
                "  collection VARCHAR(255) NOT NULL,\n" +
                "  name VARCHAR(255) NOT NULL,\n" +
                "  query VARCHAR(255) NOT NULL,\n" +
                "  UNIQUE (project, collection, name)\n" +
                ")")
        void createTableTables();

        @SqlUpdate("CREATE TABLE IF NOT EXISTS collection_schema (\n" +
                "  project VARCHAR(255) NOT NULL,\n" +
                "  collection VARCHAR(255) NOT NULL,\n" +
                "  schema BINARY NOT NULL,\n" +
                "  query VARCHAR(255) NOT NULL,\n" +
                "  UNIQUE (project, collection, name)\n" +
                ")")
        void createTableTables2();

        @SqlUpdate("DELETE FROM collection_schema\n" +
                "WHERE catalog_name = :catalogName\n" +
                "  AND schema_name = :schemaName\n" +
                "  AND table_name = :tableName")
        int dropView(
                @Bind("catalogName") String catalogName,
                @Bind("schemaName") String schemaName,
                @Bind("tableName") String tableName);

}
