package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.ConfigBinder;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.swagger.models.Tag;
import org.rakam.Mapper;
import org.rakam.analysis.ConfigManager;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SyncEventMapper;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.user.mailbox.MailBoxWebSocketService;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.report.EmailClientConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.util.ConditionalModule;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.rakam.analysis.InternalConfig.USER_TYPE;

@AutoService(RakamModule.class)
@ConditionalModule(config = "plugin.user.enabled", value = "true")
public class UserModule
        extends RakamModule {
    private Map<String, Class<? extends UserActionService>> actionList = ImmutableMap.<String, Class<? extends UserActionService>>builder()
            .put("email", UserEmailActionService.class)
            .build();

    @Override
    protected void setup(Binder binder) {
        Multibinder.newSetBinder(binder, UserPropertyMapper.class);

        Multibinder<WebSocketService> webSocketServices = Multibinder.newSetBinder(binder, WebSocketService.class);
        webSocketServices.addBinding().to(MailBoxWebSocketService.class).in(Scopes.SINGLETON);

        binder.bind(UserStorageListener.class).asEagerSingleton();

        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(EmailClientConfig.class);

        Multibinder<Tag> tagMultibinder = Multibinder.newSetBinder(binder, Tag.class);
        tagMultibinder.addBinding()
                .toInstance(new Tag().name("user").description("User")
                        .externalDocs(MetadataConfig.centralDocs));
        tagMultibinder.addBinding()
                .toInstance(new Tag().name("user-action").description("User Action")
                        .externalDocs(MetadataConfig.centralDocs));

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);

        Multibinder<UserActionService> userAction = Multibinder.newSetBinder(binder, UserActionService.class);
        Iterable<String> actionList = userPluginConfig.getActionList();
        if (actionList != null) {
            for (String actionName : actionList) {
                Class<? extends UserActionService> implementation = this.actionList.get(actionName);
                userAction.addBinding().to(implementation).in(Scopes.SINGLETON);
                httpServices.addBinding().to(implementation);
            }
        }

        if (userPluginConfig.getStorageModule() != null) {
            binder.bind(UserHttpService.class).asEagerSingleton();

            httpServices.addBinding().to(UserUtilHttpService.class);
            httpServices.addBinding().to(UserHttpService.class).in(Scopes.SINGLETON);
        }

        if (!userPluginConfig.getEnableUserMapping()) {
            Multibinder<UserPropertyMapper> userPropertyMappers = Multibinder.newSetBinder(binder, UserPropertyMapper.class);
            Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);

            eventMappers.addBinding().to(UserIdCheckEventMapper.class).in(Scopes.SINGLETON);
            userPropertyMappers.addBinding().to(UserIdCheckEventMapper.class).in(Scopes.SINGLETON);
        }
    }

    @Override
    public String name() {
        return "Customer Analytics Module";
    }

    @Override
    public String description() {
        return "Analyze your users";
    }

    public static class UserStorageListener {
        private final Optional<UserStorage> storage;
        private final Optional<UserMailboxStorage> mailboxStorage;
        private final ConfigManager configManager;

        @Inject
        public UserStorageListener(Optional<UserStorage> storage, ConfigManager configManager, Optional<UserMailboxStorage> mailboxStorage) {
            this.storage = storage;
            this.mailboxStorage = mailboxStorage;
            this.configManager = configManager;
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
            FieldType type = configManager.getConfig(event.project, USER_TYPE.name(), FieldType.class);

            if (type != null) {
                if (mailboxStorage.isPresent()) {
                    mailboxStorage.get().createProjectIfNotExists(event.project, type.isNumeric());
                }
                if (storage.isPresent()) {
                    storage.get().createProjectIfNotExists(event.project, type.isNumeric());
                }
            }
        }
    }

    public static class UserPrecomputationListener {
        private final AbstractUserService service;

        @Inject
        public UserPrecomputationListener(AbstractUserService service) {
            this.service = service;
        }

        @Subscribe
        public void onCreateFields(SystemEvents.CollectionFieldCreatedEvent event) {
            if (event.fields.stream().anyMatch(f -> f.getName().equals("_user"))) {
                createInternal(event.project, event.collection);
            }
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
            if (event.fields.stream().anyMatch(f -> f.getName().equals("_user"))) {
                createInternal(event.project, event.collection);
            }
        }

        private void createInternal(String project, String collection) {
            if (collection != null) {
                try {
//                    continuousQueryService.get(project, "_users_daily_" + collection);
                } catch (RakamException e) {
                    try {
                        service.preCalculate(project, new AbstractUserService.PreCalculateQuery(collection, null));
                    } catch (RakamException e1) {
                    }
                }
            }

            try {
//                continuousQueryService.get(project, "_users_daily");
            } catch (RakamException e) {
                try {
                    service.preCalculate(project, new AbstractUserService.PreCalculateQuery(null, null));
                } catch (RakamException e1) {
                }
            }
        }
    }

    @Mapper(name = "User Id Checker Event mapper", description = "Checks whether the event has _user attribute or not.")
    public static class UserIdCheckEventMapper
            implements SyncEventMapper, UserPropertyMapper {

        @Override
        public List<Cookie> map(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
            if (event.properties().get("_user") == null) {
                throw new RakamException("_user cannot be null", BAD_REQUEST);
            }

            return null;
        }

        @Override
        public List<Cookie> map(String project, List<? extends ISingleUserBatchOperation> user, RequestParams requestParams, InetAddress sourceAddress) {
            for (ISingleUserBatchOperation operation : user) {
                if (operation.getUser() == null) {
                    throw new RakamException("_user cannot be null", BAD_REQUEST);
                }
            }


            return null;
        }
    }
}
