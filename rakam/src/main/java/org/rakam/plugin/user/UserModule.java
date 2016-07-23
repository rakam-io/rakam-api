package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.ConfigBinder;
import io.swagger.models.Tag;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.InternalConfig;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.FieldType;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.user.mailbox.MailBoxWebSocketService;
import org.rakam.plugin.user.mailbox.UserMailboxActionService;
import org.rakam.plugin.user.mailbox.UserMailboxHttpService;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.report.EmailClientConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.ConditionalModule;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.util.Map;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

@AutoService(RakamModule.class)
@ConditionalModule(config = "plugin.user.enabled", value = "true")
public class UserModule
        extends RakamModule
{

    private Map<String, Class<? extends UserActionService>> actionList = ImmutableMap.<String, Class<? extends UserActionService>>builder()
            .put("email", UserEmailActionService.class)
            .build();

    @Override
    protected void setup(Binder binder)
    {
        Multibinder.newSetBinder(binder, UserPropertyMapper.class);

        Multibinder<WebSocketService> webSocketServices = Multibinder.newSetBinder(binder, WebSocketService.class);
        webSocketServices.addBinding().to(MailBoxWebSocketService.class).in(Scopes.SINGLETON);

        binder.bind(UserStorageListener.class).asEagerSingleton();
//        binder.bind(UserPrecomputationListener.class).asEagerSingleton();
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
            httpServices.addBinding().to(UserUtilHttpService.class);
            httpServices.addBinding().to(UserHttpService.class).in(Scopes.SINGLETON);
        }

        if (userPluginConfig.isMailboxEnabled()) {
            httpServices.addBinding().to(UserMailboxHttpService.class).in(Scopes.SINGLETON);
            httpServices.addBinding().to(UserMailboxActionService.class).in(Scopes.SINGLETON);
            userAction.addBinding().to(UserMailboxActionService.class);

            tagMultibinder.addBinding()
                    .toInstance(new Tag().name("user-mailbox").description("")
                            .externalDocs(MetadataConfig.centralDocs));
        }
    }

    @Override
    public String name()
    {
        return "Customer Analytics Module";
    }

    @Override
    public String description()
    {
        return "Analyze your users";
    }

    public static class UserStorageListener
    {

        private final Optional<UserStorage> storage;
        private final Optional<UserMailboxStorage> mailboxStorage;
        private final ConfigManager configManager;

        @Inject
        public UserStorageListener(Optional<UserStorage> storage, ConfigManager configManager, Optional<UserMailboxStorage> mailboxStorage)
        {
            this.storage = storage;
            this.mailboxStorage = mailboxStorage;
            this.configManager = configManager;
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event)
        {
            FieldType type = configManager.getConfig(event.project,
                    InternalConfig.USER_TYPE.name(), FieldType.class);

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

    public static class UserPrecomputationListener
    {
        private final AbstractUserService service;
        private final ContinuousQueryService continuousQueryService;

        @Inject
        public UserPrecomputationListener(AbstractUserService service, ContinuousQueryService continuousQueryService)
        {
            this.service = service;
            this.continuousQueryService = continuousQueryService;
        }

        @Subscribe
        public void onCreateFields(SystemEvents.CollectionFieldCreatedEvent event)
        {
            if (event.fields.stream().anyMatch(f -> f.getName().equals("_user"))) {
                createInternal(event.project, event.collection);
            }
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event)
        {
            if (event.fields.stream().anyMatch(f -> f.getName().equals("_user"))) {
                createInternal(event.project, event.collection);
            }
        }

        private void createInternal(String project, String collection)
        {
            try {
                if (collection != null) {
                    try {
                        continuousQueryService.get(project, "_users_daily_" + collection);
                    }
                    catch (RakamException e) {
                        try {
                            service.precalculate(project, new AbstractUserService.PreCalculateQuery(collection, null));
                        }
                        catch (RakamException e1) {
                        }
                    }
                }

                try {
                    continuousQueryService.get(project, "_users_daily");
                }
                catch (RakamException e) {
                    try {
                        service.precalculate(project, new AbstractUserService.PreCalculateQuery(null, null));
                    }
                    catch (RakamException e1) {
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
