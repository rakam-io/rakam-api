package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.*;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.report.EmailClientConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.ScheduledEmailService.ScheduledEmailTask.TaskType;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.ui.user.WebUserService;
import org.rakam.util.JsonHelper;
import org.rakam.util.MailSender;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.rakam.util.lock.LockService;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.util.ByteArrayDataSource;
import javax.ws.rs.Path;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.format;
import static java.time.format.TextStyle.SHORT;
import static java.util.Locale.US;

@Path("/ui/scheduled-email")
@IgnoreApi
public class ScheduledEmailService
        extends HttpService {
    private final static Logger LOGGER = Logger.get(ScheduledEmailService.class);
    private static final Mustache template;

    static {
        MustacheFactory mf = new DefaultMustacheFactory();
        try {
            template = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/splash_screen_capture.lua"), Charsets.UTF_8)), "screen_capture.lua");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final DBI dbi;
    private final ScheduledExecutorService scheduler;
    private final LockService lockService;
    private final MailSender mailSender;
    private final ListeningExecutorService executorService;
    private final WebUserHttpService webUserHttpService;
    private final URL screenCaptureService;
    private final String siteHost;

    @Inject
    public ScheduledEmailService(
            @Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource,
            LockService lockService,
            WebUserHttpService webUserHttpService,
            RakamUIConfig rakamUIConfig,
            EmailClientConfig emailConfig) {
        dbi = new DBI(dataSource);
        this.lockService = lockService;
        this.webUserHttpService = webUserHttpService;
        this.screenCaptureService = rakamUIConfig.getScreenCaptureService();
        this.mailSender = emailConfig.getMailSender();
        this.siteHost = emailConfig.getSiteUrl().getHost() + (emailConfig.getSiteUrl().getPort() == -1 ? "" : (":" + emailConfig.getSiteUrl().getPort()));
        this.scheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("scheduled-email-scheduler")
                .setUncaughtExceptionHandler((t, e) -> LOGGER.error(e))
                .build());
        executorService = MoreExecutors.listeningDecorator(new ForkJoinPool
                (Runtime.getRuntime().availableProcessors(),
                        pool -> {
                            ForkJoinWorkerThread forkJoinWorkerThread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                            forkJoinWorkerThread.setName("scheduled-email-task-worker");
                            return forkJoinWorkerThread;
                        },
                        null, true));
    }

    @PostConstruct
    public void schedule() {
        long initialDelay = millisToNextHour();

        LOGGER.info("Scheduled to run email summary tasks, the first task will run in %d minutes.", initialDelay);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                perform();
            } catch (Throwable e) {
                LOGGER.error(e);
            }
        }, initialDelay, 60, TimeUnit.MINUTES);
    }

    private long millisToNextHour() {
        LocalDateTime nextHour = LocalDateTime.now().plusHours(1).truncatedTo(ChronoUnit.HOURS);
        return LocalDateTime.now().until(nextHour, ChronoUnit.MINUTES);
    }

    private void perform() {
        List<ScheduledEmailTask> tasks;
        try (Handle handle = dbi.open()) {
            tasks = list(handle, "enabled and (last_executed_at is null or (last_executed_at < now() AT TIME ZONE 'UTC' and ((CASE WHEN date_interval LIKE 'day.%' THEN\n" +
                    "((cast(substring(date_interval, 5) as bigint) - cast(EXTRACT(DOW FROM last_executed_at) as bigint) +\n" +
                    "  (case when cast(EXTRACT(DOW FROM last_executed_at) as bigint) >= cast(substring(date_interval, 5) as bigint) then 7 else 0 end)) * INTERVAL '1 DAY')\n" +
                    "WHEN date_interval LIKE 'day_range.%' THEN\n" +
                    "(case\n" +
                    "when cast(EXTRACT(DOW FROM last_executed_at) as bigint) >= cast((string_to_array(substring(date_interval, 11), '-'))[2] as bigint) then\n" +
                    "INTERVAL '1 DAY' * (cast((string_to_array(substring(date_interval, 11), '-'))[1] as bigint) - cast(EXTRACT(DOW FROM last_executed_at) as bigint) + 7)\n" +
                    "when cast(EXTRACT(DOW FROM last_executed_at) as bigint) >= cast((string_to_array(substring(date_interval, 11), '-'))[1] as bigint) then\n" +
                    "(case when hour_of_day > cast(EXTRACT(hour FROM last_executed_at) as bigint) then  INTERVAL '0 DAY' else INTERVAL '1 DAY' end)\n" +
                    "else\n" +
                    "INTERVAL '1 DAY' * (cast((string_to_array(substring(date_interval, 11), '-'))[1] as bigint) - cast(EXTRACT(DOW FROM last_executed_at) as bigint))\n" +
                    "end)\n" +
                    "WHEN date_interval LIKE 'month.%' THEN\n" +
                    "case\n" +
                    " when extract(day from last_executed_at) > (cast(substring(date_interval, 7) as bigint))\n" +
                    " then (date_trunc('month', last_executed_at) + INTERVAL '1 MONTH') + INTERVAL '1 DAY' * (((cast(substring(date_interval, 7) as bigint)))) - last_executed_at\n" +
                    " else ((((cast(substring(date_interval, 7) as bigint)))) - extract(day from last_executed_at)) * INTERVAL '1 DAY' end\n" +
                    "ELSE NULL END) + ((INTERVAL '1 HOURS') * (case when hour_of_day > cast(EXTRACT(hour FROM last_executed_at) as bigint)\n" +
                    " then hour_of_day - cast(EXTRACT(hour FROM last_executed_at) as bigint) else hour_of_day - cast(EXTRACT(hour FROM last_executed_at) as bigint) end))\n" +
                    ") + last_executed_at < now() AT TIME ZONE 'UTC'))", query -> {
            });
        }

        LOGGER.info("Running email summary tasks, %d emails will be sent.", tasks.size());

        for (ScheduledEmailTask task : tasks) {
            try {
                LockService.Lock lock = lockService.tryLock("dashboard." + String.valueOf(task.id));

                if (lock == null) {
                    continue;
                }

                long now = Instant.now().toEpochMilli();

                send(task, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(@Nullable Void result) {
                        updateTask(task.id, lock, now, null);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        updateTask(task.id, lock, now, t);
                    }
                });
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }
    }

    private void send(ScheduledEmailTask task, FutureCallback<Void> callback)
            throws MessagingException, UnsupportedEncodingException {
        MimeBodyPart screenPart = new MimeBodyPart();
        String imageId = UUID.randomUUID().toString() + "@" +
                UUID.randomUUID().toString() + ".mail";
        screenPart.setHeader("Content-ID", "<" + imageId + ">");

        StringWriter writer;

        writer = new StringWriter();
        String path = "/" + task.project_id + "/dashboard/" + task.type_id;
        template.execute(writer, of(
                "domain", this.siteHost,
                "session", webUserHttpService.getCookieForUser(task.user_id),
                "path", path));
        String txtContent = writer.toString();

        ListenableFuture<Void> run = executorService.submit(() -> {
            try {
                URL u = new URL(screenCaptureService.toString() + "/execute");
                HttpURLConnection conn = (HttpURLConnection) u.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setRequestProperty("Content-Length", String.valueOf(txtContent.length()));

                try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
                    wr.write(JsonHelper.encodeAsBytes(ImmutableMap.of("lua_source", txtContent, "timeout", 60)));
                }
                writer.flush();

                byte[] bytes;
                if (conn.getResponseCode() == 200) {
                    bytes = ByteStreams.toByteArray(conn.getInputStream());
                } else {
                    bytes = ByteStreams.toByteArray(conn.getErrorStream());
                    throw new RuntimeException("Error while sending scheduled e-mail",
                            new RuntimeException("Id: " + task.id + " -> " + new String(bytes)));
                }

                ZonedDateTime dateTime = Instant.now().atZone(ZoneOffset.UTC);
                String month = dateTime.getMonth().getDisplayName(SHORT, US);
                int day = dateTime.getDayOfMonth();
                String weekDay = dateTime.getDayOfWeek().getDisplayName(SHORT, US);
                DataSource dataSource = new ByteArrayDataSource(bytes, "image/png");
                screenPart.setDataHandler(new DataHandler(dataSource));
                screenPart.setFileName("dashboard.png");
                screenPart.setDisposition(MimeBodyPart.INLINE);

                String title = format("[Rakam] %s — %s, %s %d%s", task.name, weekDay, month, day, getDayOfMonthSuffix(day));

                mailSender.sendMail(task.emails, title,
                        "Please view HTML version of the email, it contains the dashboard screenshot that is sent from Rakam UI.",
                        Optional.of("<a href=\"https://" + siteHost + path + "\"> " +
                                "<img alt=\"Rakam dashboard screenshot\" src=\"cid:" + imageId + "\" /></a>" +
                                " <div style=\"color: white;\">Inline dashboard</div> <!-- This div allows the screenshot to be resized in android gmail, and shows as preview text. -->"),
                        Stream.of(screenPart));
                return null;
            } catch (IOException | MessagingException e) {
                throw Throwables.propagate(e);
            }
        });

        Futures.addCallback(run, callback);
    }

    private void updateTask(int id, LockService.Lock lock, long now, Throwable ex) {
        if (ex == null) {
            try (Handle handle = dbi.open()) {
                handle.createStatement("UPDATE scheduled_email SET last_executed_at = now() at time zone 'utc' WHERE id = :id")
                        .bind("id", id).execute();
            } finally {
                lock.release();
            }
        } else {
            lock.release();
        }

        long gapInMillis = System.currentTimeMillis() - now;
        if (ex != null) {
            LOGGER.error(ex, format("Failed to send scheduled email in %d ms : %s", gapInMillis, ex.getMessage()));
        }
    }

    private String getDayOfMonthSuffix(final int n) {
        if (n >= 11 && n <= 13) {
            return "th";
        }
        switch (n % 10) {
            case 1:
                return "st";
            case 2:
                return "nd";
            case 3:
                return "rd";
            default:
                return "th";
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/create")
    @ProtectEndpoint(writeOperation = true)
    public ScheduledEmailTask create(
            @Named("user_id") UIPermissionParameterProvider.Project project,
            @ApiParam("name") String name,
            @ApiParam("date_interval") String date_interval,
            @ApiParam("hour_of_day") int hour_of_day,
            @ApiParam("type") TaskType type,
            @ApiParam("type_id") int type_id,
            @ApiParam("emails") List<String> emails) {
        try (Handle handle = dbi.open()) {
            int id = handle.createQuery("INSERT INTO scheduled_email (project_id, user_id, date_interval, hour_of_day, name, type, type_id, emails, enabled) " +
                    "VALUES (:project, :user_id, :date_interval, :hour_of_day, :name, :type, :type_id, :emails, true) RETURNING id")
                    .bind("project", project.project)
                    .bind("user_id", project.userId)
                    .bind("date_interval", date_interval)
                    .bind("hour_of_day", hour_of_day)
                    .bind("name", name)
                    .bind("type", type)
                    .bind("type_id", type_id)
                    .bind("emails", handle.getConnection().createArrayOf("text", emails.toArray()))
                    .map(IntegerMapper.FIRST).first();

            return new ScheduledEmailTask(
                    id, name, date_interval,
                    hour_of_day, type, type_id, emails,
                    true, null, project.userId, project.project);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/test")
    public CompletableFuture<SuccessMessage> test(
            @Named("user_id") UIPermissionParameterProvider.Project project,
            @ApiParam("id") int id) {
        ScheduledEmailTask task;
        String email;
        try (Handle handle = dbi.open()) {
            task = list(handle, "project_id = :project and user_id = :user and id = :id",
                    query -> query.bind("project", project.project)
                            .bind("user", project.userId)
                            .bind("id", id)).get(0);
            email = handle.createQuery("select email from web_user where id = :id")
                    .bind("id", project.userId).map(StringMapper.FIRST).first();
        }

        CompletableFuture<SuccessMessage> success = new CompletableFuture<>();

        try {
            task.emails = ImmutableList.of(email);
            send(task, new FutureCallback<Void>() {
                @Override
                public void onSuccess(@Nullable Void result) {
                    success.complete(SuccessMessage.success());
                }

                @Override
                public void onFailure(Throwable t) {
                    success.completeExceptionally(t);
                }
            });
        } catch (MessagingException | UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }

        return success;
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/update")
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage update(
            @Named("user_id") UIPermissionParameterProvider.Project project,
            @ApiParam("id") int id,
            @ApiParam(value = "name", required = false) String name,
            @ApiParam(value = "date_interval", required = false) String date_interval,
            @ApiParam(value = "hour_of_day", required = false) Integer hour_of_day,
            @ApiParam(value = "type", required = false) TaskType type,
            @ApiParam(value = "type_id", required = false) Integer type_id,
            @ApiParam(value = "enabled", required = false) Boolean enabled,
            @ApiParam(value = "emails", required = false) List<String> emails) {
        try (Handle handle = dbi.open()) {
            ArrayList<String> objects = new ArrayList<>();

            if (name != null) {
                objects.add("name = :name");
            }
            if (enabled != null) {
                objects.add("enabled = :enabled");
            }
            if (date_interval != null) {
                objects.add("date_interval = :date_interval");
            }
            if (hour_of_day != null) {
                objects.add("hour_of_day = :hour_of_day");
            }
            if (type != null) {
                objects.add("type = :type");
            }
            if (type_id != null) {
                objects.add("type_id = :type_id");
            }
            if (emails != null) {
                objects.add("emails = :emails");
            }

            Update bind = handle.createStatement(format("UPDATE scheduled_email SET %s WHERE project_id = :project AND user_id = :user_id AND id = :id",
                    objects.stream().collect(Collectors.joining(", "))))
                    .bind("project", project.project)
                    .bind("id", id)
                    .bind("user_id", project.userId);

            if (name != null) {
                bind.bind("name", name);
            }
            if (date_interval != null) {
                bind.bind("date_interval", date_interval);
            }
            if (hour_of_day != null) {
                bind.bind("hour_of_day", hour_of_day);
            }
            if (type != null) {
                bind.bind("type", type.name());
            }
            if (enabled != null) {
                bind.bind("enabled", enabled);
            }
            if (type_id != null) {
                bind.bind("type_id", type_id);
            }
            if (emails != null) {
                bind.bind("emails", handle.getConnection()
                        .createArrayOf("text", emails.toArray()));
            }

            bind.execute();

            return SuccessMessage.success();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/delete")
    public SuccessMessage delete(@Named("user_id") UIPermissionParameterProvider.Project project, @ApiParam("id") int id) {
        try (Handle handle = dbi.open()) {
            int count = handle.createStatement("DELETE FROM scheduled_email " +
                    "WHERE id = :id AND project_id = :project and user_id = :user")
                    .bind("id", id)
                    .bind("user", project.userId)
                    .bind("project", project.project).execute();

            if (count > 0) {
                return SuccessMessage.success();
            }

            throw new RakamException(HttpResponseStatus.NOT_FOUND);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/list")
    public List<ScheduledEmailTask> list(@Named("user_id") UIPermissionParameterProvider.Project project) {
        try (Handle handle = dbi.open()) {
            return list(handle, "project_id = :project and user_id = :user",
                    query -> query.bind("project", project.project).bind("user", project.userId));
        }
    }

    private List<ScheduledEmailTask> list(Handle handle, String predicate, Consumer<Query> queryConsumer) {
        Query<Map<String, Object>> query = handle.createQuery("SELECT id, name, date_interval, hour_of_day, type, type_id, emails, enabled, last_executed_at, user_id, project_id " +
                "FROM scheduled_email WHERE " + predicate);
        queryConsumer.accept(query);

        return query
                .map((index, r, ctx) -> {
                    return new ScheduledEmailTask(
                            r.getInt(1), r.getString(2),
                            r.getString(3), r.getInt(4),
                            TaskType.valueOf(r.getString(5)),
                            r.getInt(6), Arrays.asList((String[]) r.getArray(7).getArray()),
                            r.getBoolean(8), r.getTimestamp(9) == null ? null : r.getTimestamp(9).toInstant(),
                            r.getInt(10), r.getInt(11));
                }).list();
    }

    public static class ScheduledEmailTask {
        public final int id;
        public final String name;
        public final String date_interval;
        public final int hour_of_day;
        public final TaskType type;
        public final int type_id;
        public final boolean enabled;
        public final Instant last_executed_at;
        public final int user_id;
        public final int project_id;
        public List<String> emails;

        @JsonCreator
        public ScheduledEmailTask(
                @ApiParam("id") int id,
                @ApiParam("name") String name,
                @ApiParam("date_interval") String dateInterval,
                @ApiParam("hour_of_day") int hour_of_day,
                @ApiParam("type") TaskType type,
                @ApiParam("type_id") int typeId,
                @ApiParam("emails") List<String> emails,
                @ApiParam("enabled") boolean enabled,
                @ApiParam("last_executed_at") Instant last_executed_at,
                @ApiParam("user_id") int user_id,
                @ApiParam("project_id") int project_id) {
            this.id = id;
            this.name = name;
            this.date_interval = dateInterval;
            this.hour_of_day = hour_of_day;
            this.type = type;
            this.type_id = typeId;
            this.emails = emails;
            this.enabled = enabled;
            this.last_executed_at = last_executed_at;
            this.user_id = user_id;
            this.project_id = project_id;
        }

        public enum TaskType {
            DASHBOARD;

            @JsonCreator
            public static TaskType get(String name) {
                return valueOf(name.toUpperCase());
            }

            @JsonProperty
            public String value() {
                return name();
            }
        }
    }
}
