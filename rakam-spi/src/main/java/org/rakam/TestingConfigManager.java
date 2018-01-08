package org.rakam;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.rakam.analysis.ConfigManager;

public class TestingConfigManager
        implements ConfigManager {
    Table<String, String, Object> table;

    public TestingConfigManager() {
        this.table = HashBasedTable.create();
    }

    @Override
    public synchronized <T> T getConfig(String project, String configName, Class<T> clazz) {
        return (T) table.get(project, configName);
    }

    @Override
    public synchronized <T> void setConfig(String project, String configName, T clazz) {
        if (clazz == null) {
            table.remove(project, configName);
        } else {
            table.put(project, configName, clazz);
        }
    }

    @Override
    public synchronized <T> T setConfigOnce(String project, String configName, T clazz) {
        Object o = table.row(project).putIfAbsent(configName, clazz);
        return o == null ? clazz : (T) o;
    }

    @Override
    public void clear() {
        table.clear();
    }

    public Table<String, String, Object> getTable() {
        return table;
    }
}
