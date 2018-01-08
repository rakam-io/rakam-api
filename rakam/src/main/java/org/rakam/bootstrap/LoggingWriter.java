package org.rakam.bootstrap;

/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

class LoggingWriter extends StringWriter {
    private final Logger logger;
    private final Type type;

    public LoggingWriter(Logger logger, Type type) {
        this.logger = logger;
        this.type = type;
    }

    @Override
    public void close()
            throws IOException {
        flush();
        super.close();
    }

    @Override
    public void flush() {
        BufferedReader in = new BufferedReader(new StringReader(getBuffer().toString()));
        for (; ; ) {
            try {
                String line = in.readLine();
                if (line == null) {
                    break;
                }

                switch (type) {
                    default:
                    case DEBUG: {
                        if (logger.isDebugEnabled()) {
                            logger.debug(line);
                        }
                        break;
                    }

                    case INFO: {
                        if (logger.isInfoEnabled()) {
                            logger.info(line);
                        }
                        break;
                    }
                }
            } catch (IOException e) {
                throw new Error(e); // should never get here
            }
        }

        getBuffer().setLength(0);
    }

    public void printMessage(String message, Object... args) {
        write(String.format(message, args) + "\n");
    }

    public enum Type {
        DEBUG,
        INFO
    }
}

