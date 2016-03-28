package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.swagger.models.Tag;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.user.mailbox.MailBoxWebSocketService;
import org.rakam.plugin.user.mailbox.UserMailboxActionService;
import org.rakam.plugin.user.mailbox.UserMailboxHttpService;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.EmailClientConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.util.ConditionalModule;

import javax.inject.Inject;
import java.util.Map;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkProject;

@AutoService(RakamModule.class)
@ConditionalModule(config="plugin.user.enabled", value = "true")
public class UserModule extends RakamModule {

    Map<String, Class<? extends UserActionService>> actionList = ImmutableMap.<String, Class<? extends UserActionService>>builder()
                    .put("email", UserEmailActionService.class)
                    .build();

    @Override
    protected void setup(Binder binder) {
        Multibinder.newSetBinder(binder, UserPropertyMapper.class);

        Multibinder<WebSocketService> webSocketServices = Multibinder.newSetBinder(binder, WebSocketService.class);
        webSocketServices.addBinding().to(MailBoxWebSocketService.class).in(Scopes.SINGLETON);

        binder.bind(UserStorageListener.class).asEagerSingleton();
        binder.bind(UserCollectionFieldListener.class).asEagerSingleton();
        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);
        bindConfig(binder).to(EmailClientConfig.class);

        Multibinder<Tag> tagMultibinder = Multibinder.newSetBinder(binder, Tag.class);
        tagMultibinder.addBinding()
                .toInstance(new Tag().name("user").description("User module for Rakam")
                        .externalDocs(MetadataConfig.centralDocs));

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);

        Multibinder<UserActionService> userAction = Multibinder.newSetBinder(binder, UserActionService.class);
        Iterable<String> actionList = userPluginConfig.getActionList();
        if(actionList != null) {
            for (String actionName : actionList) {
                Class<? extends UserActionService> implementation = this.actionList.get(actionName);
                userAction.addBinding().to(implementation).in(Scopes.SINGLETON);
                httpServices.addBinding().to(implementation);
            }
        }

        if (userPluginConfig.getStorageModule() != null) {
            httpServices.addBinding().to(UserHttpService.class).in(Scopes.SINGLETON);
        }

        if(userPluginConfig.isMailboxEnabled()) {
            httpServices.addBinding().to(UserMailboxHttpService.class).in(Scopes.SINGLETON);
            httpServices.addBinding().to(UserMailboxActionService.class).in(Scopes.SINGLETON);
            userAction.addBinding().to(UserMailboxActionService.class);

            tagMultibinder.addBinding()
                    .toInstance(new Tag().name("user-mailbox").description("")
                            .externalDocs(MetadataConfig.centralDocs));
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
        private final PostgresqlQueryExecutor queryExecutor;

        @Inject
        public UserStorageListener(com.google.common.base.Optional<UserStorage> storage, com.google.common.base.Optional<UserMailboxStorage> mailboxStorage, PostgresqlQueryExecutor queryExecutor) {
            this.storage = storage;
            this.mailboxStorage = mailboxStorage;
            this.queryExecutor = queryExecutor;
        }

        @Subscribe
        public void onCreateProject(SystemEvents.ProjectCreatedEvent event) {
            checkProject(event.project);
            // if event.store is not postgresql, schema may not exist.
            queryExecutor.executeRawStatement(format("CREATE SCHEMA IF NOT EXISTS %s", event.project)).getResult().join();

            if(mailboxStorage.isPresent()) {
                mailboxStorage.get().createProject(event.project);
            }
            if(storage.isPresent()) {
                storage.get().createProject(event.project);
            }
        }
    }

    public static class UserCollectionFieldListener {
        private final Metastore metastore;

        @Inject
        public UserCollectionFieldListener(Metastore metastore) {
            this.metastore = metastore;
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
            if (!event.fields.stream().anyMatch(f -> f.getName().equals("_user"))) {
                FieldType userFieldType = metastore.getCollections(event.project).entrySet()
                        .stream()
                        .flatMap(e -> e.getValue().stream()).filter(e -> e.getName().equals("_user"))
                        .filter(e -> e.getName().equals("_user")).findAny().map(f -> f.getType()).orElse(FieldType.STRING);
                metastore.getOrCreateCollectionFieldList(event.project, event.collection, ImmutableSet.of(new SchemaField("_user", userFieldType)));
            }
        }
    }
}
