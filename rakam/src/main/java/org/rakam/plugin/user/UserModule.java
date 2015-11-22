package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.swagger.models.Tag;
import org.rakam.MetadataConfig;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;

import javax.inject.Inject;
import java.util.Map;

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
        Multibinder<WebSocketService> webSocketServices = Multibinder.newSetBinder(binder, WebSocketService.class);
        webSocketServices.addBinding().to(MailBoxWebSocketService.class).in(Scopes.SINGLETON);

        binder.bind(UserStorageListener.class).asEagerSingleton();
        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        Multibinder<Tag> tagMultibinder = Multibinder.newSetBinder(binder, Tag.class);
        tagMultibinder.addBinding()
                .toInstance(new Tag().name("user").description("User module for Rakam")
                        .externalDocs(MetadataConfig.centralDocs));

        Multibinder<UserActionService> userAction = Multibinder.newSetBinder(binder, UserActionService.class);
        Iterable<String> actionList = userPluginConfig.getActionList();
        if(actionList != null) {
            for (String actionName : actionList) {
                userAction.addBinding().to(this.actionList.get(actionName)).in(Scopes.SINGLETON);
            }
        }

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);

        if (userPluginConfig.getStorageModule() != null) {
            httpServices.addBinding().to(UserHttpService.class).in(Scopes.SINGLETON);
        }

        if(userPluginConfig.isMailboxEnabled()) {
            httpServices.addBinding().to(UserMailboxHttpService.class).in(Scopes.SINGLETON);

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
}
