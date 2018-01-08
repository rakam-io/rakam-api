package org.rakam.aws;

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;

import static com.google.common.collect.ImmutableList.of;
import static java.lang.String.format;
import static java.lang.System.getProperty;

public class DynamodbUtil {
    private final static Logger LOGGER = Logger.get(DynamodbUtil.class);

    public static int randomPort()
            throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static DynamodbProcess createDynamodbProcess()
            throws Exception {
        int randomPort = randomPort();
        Path mainDir = new File(getProperty("user.dir"), ".test/dynamodb").toPath();

        Process dynamodbServer = new ProcessBuilder(of("java", format("-Djava.library.path=%s",
                mainDir.resolve("DynamoDBLocal_lib").toFile().getAbsolutePath()),
                "-jar", mainDir.resolve("DynamoDBLocal.jar").toFile().getAbsolutePath(),
                "-inMemory", "--port", Integer.toString(randomPort)))
                .start();

        LOGGER.info("Dynamodb local started at %d port", randomPort);

        return new DynamodbProcess(dynamodbServer, randomPort);
    }

    public static class DynamodbProcess {
        public final Process process;
        public final int port;

        public DynamodbProcess(Process process, int port) {
            this.process = process;
            this.port = port;
        }
    }
}
