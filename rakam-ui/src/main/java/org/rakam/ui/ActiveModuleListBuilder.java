package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.EventExplorerConfig;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.RealTimeConfig;
import org.rakam.plugin.UserPluginConfig;

import javax.inject.Inject;

class ActiveModuleListBuilder {
    private final UserPluginConfig userPluginConfig;
    private final RealTimeConfig realtimeConfig;
    private final EventStreamConfig eventStreamConfig;
    private final EventExplorerConfig eventExplorerConfig;
    private final UserPluginConfig userStorage;

    @Inject
    public ActiveModuleListBuilder(UserPluginConfig userPluginConfig, RealTimeConfig realtimeConfig, EventStreamConfig eventStreamConfig, EventExplorerConfig eventExplorerConfig, UserPluginConfig userStorage) {
       this.userPluginConfig = userPluginConfig;
       this.realtimeConfig = realtimeConfig;
       this.eventStreamConfig = eventStreamConfig;
       this.eventExplorerConfig = eventExplorerConfig;
       this.userStorage = userStorage;
    }

    public ActiveModuleList build() {
        return new ActiveModuleList(userPluginConfig, realtimeConfig, eventStreamConfig, eventExplorerConfig, userStorage);
    }

    public static class ActiveModuleList {
        @JsonProperty
        public final boolean userStorage;
        @JsonProperty
        public final boolean userMailbox;
        @JsonProperty
        public final boolean funnelAnalysisEnabled;
        @JsonProperty
        public final boolean retentionAnalysisEnabled;
        @JsonProperty
        public final boolean eventExplorer;
        @JsonProperty
        public final boolean realtime;
        @JsonProperty
        public final boolean eventStream;
        @JsonProperty
        public final boolean userStorageEventFilter;

        private ActiveModuleList(UserPluginConfig userPluginConfig, RealTimeConfig realtimeConfig, EventStreamConfig eventStreamConfig, EventExplorerConfig eventExplorerConfig, UserPluginConfig userStorage) {
            this.userStorage = userPluginConfig.getStorageModule() != null;
            this.userMailbox = userPluginConfig.getMailBoxStorageModule() != null;
            this.funnelAnalysisEnabled = userPluginConfig.isFunnelAnalysisEnabled();
            this.retentionAnalysisEnabled = userPluginConfig.isRetentionAnalysisEnabled();
            this.eventExplorer = eventExplorerConfig.isEventExplorerEnabled();
            this.realtime = realtimeConfig.isRealtimeModuleEnabled();
            this.eventStream = eventStreamConfig.isEventStreamEnabled();
            this.userStorageEventFilter = userStorage.getStorageModule() != null;
        }
    }
}
