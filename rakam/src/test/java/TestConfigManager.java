import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.rakam.analysis.ConfigManager;

import java.util.function.Function;

public class TestConfigManager implements ConfigManager {
    Table<String, String, Object> table;

    public TestConfigManager() {
        this.table = HashBasedTable.create();
    }

    @Override
    public synchronized  <T> T getConfig(String project, String configName, Class<T> clazz) {
        return (T) table.get(project, configName);
    }

    @Override
    public synchronized  <T> void setConfig(String project, String configName, T clazz) {
        table.put(project, configName, clazz);
    }

    @Override
    public synchronized <T> T setConfigOnce(String project, String configName, T clazz) {
        table.column(project).putIfAbsent(configName, clazz);
        return null;
    }

    @Override
    public synchronized <T> T computeConfig(String project, String configName, Function<T, T> mapper, Class<T> clazz) {
        T t = (T) table.get(project, configName);
        table.put(project, configName, mapper.apply(t));
        return t;
    }
}
