package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.skife.jdbi.v2.ResultIterator;
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.format;
import static org.rakam.util.JsonHelper.encode;

@Path("/ui/scheduled-email")
@IgnoreApi
public class ScheduledEmailService
        extends HttpService
{
    private final static Logger LOGGER = Logger.get(ScheduledEmailService.class);

    private final DBI dbi;
    private final ScheduledExecutorService scheduler;
    private final LockService lockService;
    private final MailSender mailSender;
    private static final Mustache template;
    private final ListeningExecutorService executorService;
    private final WebUserHttpService webUserHttpService;
    private final URL screenCaptureService;

    static {
        MustacheFactory mf = new DefaultMustacheFactory();
        try {
            template = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/splash_screen_capture.lua"), Charsets.UTF_8)), "screen_capture.lua");
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Inject
    public ScheduledEmailService(
            @Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource,
            LockService lockService,
            WebUserHttpService webUserHttpService,
            RakamUIConfig rakamUIConfig,
            EmailClientConfig emailConfig)
    {
        dbi = new DBI(dataSource);
        this.lockService = lockService;
        this.webUserHttpService = webUserHttpService;
        this.screenCaptureService = rakamUIConfig.getScreenCaptureService();
        this.mailSender = emailConfig.getMailSender();
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
    public void setup()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS scheduled_email (" +
                    "  id SERIAL PRIMARY KEY," +
                    "  project_id INTEGER REFERENCES web_user_project(id),\n" +
                    "  user_id INTEGER REFERENCES web_user(id),\n" +
                    "  name VARCHAR(255) NOT NULL," +
                    "  date_interval VARCHAR(100) NOT NULL," +
                    "  hour_of_day INTEGER NOT NULL," +
                    "  type VARCHAR(100)," +
                    "  type_id BIGINT," +
                    "  created_at timestamp without time zone default (now() at time zone 'utc')," +
                    "  last_executed_at timestamp without time zone," +
                    "  enabled BOOL NOT NULL DEFAULT true," +
                    "  emails VARCHAR(100)[]" +
                    "  )")
                    .execute();
        }
    }

    @PostConstruct
    public void schedule()
    {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                perform();
            }
            catch (Exception e) {
                LOGGER.error(e);
            }
        }, millisToNextHour(), 60, TimeUnit.MINUTES);
    }

    private long millisToNextHour() {
        LocalDateTime nextHour = LocalDateTime.now().plusHours(1).truncatedTo(ChronoUnit.HOURS);
        return LocalDateTime.now().until(nextHour, ChronoUnit.MINUTES);
    }

    private void perform()
    {
        List<ScheduledEmailTask> tasks;
        try (Handle handle = dbi.open()) {
            tasks = list(handle, "last_executed_at is null or ((CASE WHEN date_interval LIKE 'day.%' THEN\n" +
                    "(mod(cast(substring(date_interval, 5) as bigint) - cast(EXTRACT(DOW FROM last_executed_at) as bigint) + 7, 7) * INTERVAL '1 DAY')\n" +
                    "WHEN date_interval LIKE 'day_range.%' THEN\n" +
                    "(case \n" +
                    "when cast(EXTRACT(DOW FROM last_executed_at) as bigint) > cast((string_to_array(substring(date_interval, 11), '-'))[2] as bigint) then\n" +
                    "INTERVAL '1 DAY' * (cast(EXTRACT(DOW FROM last_executed_at) as bigint) - cast((string_to_array(substring(date_interval, 10), '-'))[1] as bigint))\n" +
                    "when cast(EXTRACT(DOW FROM last_executed_at) as bigint) > cast((string_to_array(substring(date_interval, 11), '-'))[1] as bigint) then\n" +
                    "INTERVAL '0 DAY'\n" +
                    "else \n" +
                    "INTERVAL '1 DAY' * (cast((string_to_array(substring(date_interval, 11), '-'))[1] as bigint) - cast(EXTRACT(DOW FROM last_executed_at) as bigint))\n" +
                    "end)\n" +
                    "WHEN date_interval LIKE 'month.%' THEN\n" +
                    "(date_trunc('month', last_executed_at) + INTERVAL '1 MONTH') + INTERVAL '1 DAY' * (((cast(substring(date_interval, 7) as bigint)))) - last_executed_at\n" +
                    "ELSE NULL END) + ((INTERVAL '1 HOURS') * mod(hour_of_day - cast(EXTRACT(hour FROM last_executed_at) as bigint) + 24, 24)) + last_executed_at > now() AT TIME ZONE 'UTC')", query -> {
            });
        }


        for (ScheduledEmailTask task : tasks) {
            try {
                LockService.Lock lock = lockService.tryLock("dashboard." + String.valueOf(task.id));

                if (lock == null) {
                    continue;
                }
                long now = Instant.now().toEpochMilli();

                MimeBodyPart screenPart = new MimeBodyPart();
                String imageId = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".mail";
                screenPart.setHeader("Content-ID", "<" + imageId + ">");

                StringWriter writer;

                writer = new StringWriter();
                String path = "/dashboard/" + task.type_id;
                Map<String, Object> project;
                try (Handle handle = dbi.open()) {
                    project = handle.createQuery("select project, api_url from web_user_project where id = :id")
                            .bind("id", task.project_id).first();
                }
                template.execute(writer, of(
                        "domain", "app.rakam.io",
                        "session", webUserHttpService.getCookieForUser(task.user_id),
                        "active_project", URLEncoder.encode(encode(of("name", project.get("project"), "apiUrl", project.get("api_url"))), "UTF-8"),
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

                        byte[] bytes;
                        if (conn.getResponseCode() == 200) {
                            bytes = ByteStreams.toByteArray(conn.getInputStream());
                        }
                        else {
                            bytes = ByteStreams.toByteArray(conn.getErrorStream());
                            LOGGER.error(new RuntimeException(new String(bytes)), "Error while sending scheduled e-mail");
                        }

                        DataSource dataSource = new ByteArrayDataSource(bytes, "image/png");
                        screenPart.setDataHandler(new DataHandler(dataSource));
                        screenPart.setFileName("dashboard.png");
                        screenPart.setDisposition(MimeBodyPart.INLINE);

                        mailSender.sendMail(task.emails, "[Rakam] - " + task.name,
                                "Please view HTML version of the email, it contains the dashboard screenshot that is sent from Rakam UI.",
                                Optional.of("<a href=\"https://app.rakam.io" + path + "\"> " +
                                        "<img alt=\"Rakam dashboard screenshot\" src=\"cid:" + imageId + "\" /></a>" +
                                        " <div style=\"color: white;\">Inline dashboard</div> <!-- This div allows the screenshot to be resized in android gmail, and shows as preview text. -->"),
                                Stream.of(screenPart));
                        return null;
                    }
                    catch (IOException | MessagingException e) {
                        throw Throwables.propagate(e);
                    }
                });

                Futures.addCallback(run, new FutureCallback<Void>()
                {
                    @Override
                    public void onSuccess(@Nullable Void result)
                    {
                        updateTask(task.id, lock, now, null);
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        updateTask(task.id, lock, now, t);
                    }
                });
            }
            catch (Exception e) {
                LOGGER.error(e);
            }
        }
    }

    private void updateTask(int id, LockService.Lock lock, long now, Throwable ex)
    {
        if (ex == null) {
            try (Handle handle = dbi.open()) {
                handle.createStatement("UPDATE scheduled_email SET last_executed_at = now() WHERE id = :id")
                        .bind("id", id).execute();
            }
            finally {
                lock.release();
            }
        }
        else {
            lock.release();
        }

        long gapInMillis = System.currentTimeMillis() - now;
        if (ex != null) {
            LOGGER.error(ex, format("Failed to send scheduled email in %d ms : %s", gapInMillis, ex.getMessage()));
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
            @ApiParam("emails") List<String> emails)
    {
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
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/update")
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage update(
            @Named("user_id") UIPermissionParameterProvider.Project project,
            @ApiParam("id") String id,
            @ApiParam(value = "name", required = false) String name,
            @ApiParam(value = "date_interval", required = false) String date_interval,
            @ApiParam(value = "hour_of_day", required = false) Integer hour_of_day,
            @ApiParam(value = "type", required = false) TaskType type,
            @ApiParam(value = "type_id", required = false) Integer type_id,
            @ApiParam(value = "emails", required = false) List<String> emails)
    {
        try (Handle handle = dbi.open()) {
            ArrayList<String> objects = new ArrayList<>();

            if (name != null) {
                objects.add("name = :name");
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

            Update bind = handle.createStatement(String.format("UPDATE scheduled_email SET %s WHERE project_id = :project AND user_id = :user_id",
                    objects.stream().collect(Collectors.joining(", "))))
                    .bind("project", project.project)
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
            if (type_id != null) {
                bind.bind("type_id", type_id);
            }
            if (emails != null) {
                bind.bind("emails", handle.getConnection().createArrayOf("text", emails.toArray()));
            }

            bind.execute();

            return SuccessMessage.success();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard")
    @Path("/delete")
    public SuccessMessage delete(@Named("user_id") UIPermissionParameterProvider.Project project, @ApiParam("id") int id)
    {
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
    public List<ScheduledEmailTask> list(@Named("user_id") UIPermissionParameterProvider.Project project)
    {
        try (Handle handle = dbi.open()) {
            return list(handle, "project_id = :project and user_id = :user",
                    query -> query.bind("project", project.project).bind("user", project.userId));
        }
    }

    private List<ScheduledEmailTask> list(Handle handle, String predicate, Consumer<Query> queryConsumer)
    {
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

    public static class ScheduledEmailTask
    {
        public final int id;
        public final String name;
        public final String date_interval;
        public final int hour_of_day;
        public final TaskType type;
        public final int type_id;
        public final List<String> emails;
        public final boolean enabled;
        public final Instant last_executed_at;
        public final int user_id;
        public final int project_id;

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
                @ApiParam("project_id") int project_id)
        {
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

        public enum TaskType
        {
            DASHBOARD;

            @JsonCreator
            public static TaskType get(String name)
            {
                return valueOf(name.toUpperCase());
            }

            @JsonProperty
            public String value()
            {
                return name();
            }
        }
    }
}
