package org.rakam.analysis;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class TestConfigManager {
    private static final String PROJECT_NAME = TestConfigManager.class.getName().replace(".", "_").toLowerCase();

    public abstract ConfigManager getConfigManager();

    @Test
    public void testSet()
            throws Exception {
        getConfigManager().setConfig(PROJECT_NAME, "test", "naber");

        assertEquals(getConfigManager().getConfig(PROJECT_NAME, "test", String.class), "naber");
    }

    @Test
    public void testSetOnce()
            throws Exception {
        getConfigManager().setConfig(PROJECT_NAME, "test", "naber");
        getConfigManager().setConfigOnce(PROJECT_NAME, "test", "naber2");

        assertEquals(getConfigManager().getConfig(PROJECT_NAME, "test", String.class), "naber");
    }

    @Test
    public void testSetOnceOtherProject()
            throws Exception {
        getConfigManager().setConfig(PROJECT_NAME, "test", "naber");
        getConfigManager().setConfigOnce(PROJECT_NAME + "i", "test", "naber2");

        assertEquals(getConfigManager().getConfig(PROJECT_NAME, "test", String.class), "naber");
        assertEquals(getConfigManager().getConfig(PROJECT_NAME + "i", "test", String.class), "naber2");
    }
}
