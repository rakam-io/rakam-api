/*
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
package org.rakam.ui.page;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.ui.RakamUIConfig;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;


public class FileBackedCustomPageDatabase implements CustomPageDatabase {
    private final File directory;

    @Inject
    public FileBackedCustomPageDatabase(RakamUIConfig config) {
        directory = config.getCustomPageBackendDirectory();
        directory.mkdirs();
    }

    private static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory()) {
                        deleteDirectory(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }
        }
        return (directory.delete());
    }

    @Override
    public void save(Integer user, int project, Page page) {
        File projectDirectory = new File(directory, Integer.toString(project));
        if (!projectDirectory.exists()) {
            projectDirectory.mkdir();
        }
        File pageDirectory = new File(projectDirectory, page.name);
        if (!pageDirectory.exists()) {
            pageDirectory.mkdir();
        }
        for (Map.Entry<String, String> entry : page.files.entrySet()) {
            try {
                // overwrite
                Files.write(new File(pageDirectory, entry.getKey()).toPath(), entry.getValue().getBytes(UTF_8), StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public List<Page> list(int project) {
        File projectDir = new File(directory, Integer.toString(project));
        String[] list = projectDir.list();
        if (list == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(list).filter(file -> new File(projectDir, file).isDirectory())
                .map(name -> new Page(name, name, null)).collect(Collectors.toList());
    }

    @Override
    public Map<String, String> get(int project, String name) {
        File dir = new File(directory, project + File.separator + name);
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException();
        }
        return Arrays.stream(dir.listFiles())
                .filter(File::isFile)
                .collect(Collectors.toMap(File::getName, file -> {
                    try {
                        return new String(Files.readAllBytes(file.toPath()));
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }));
    }

    @Override
    public InputStream getFile(int project, String name, String file) {
        File f = new File(directory, project + File.separator + name + File.separator + file);
        try {
            return new ByteArrayInputStream(Files.readAllBytes(f.toPath()));
        } catch (NoSuchFileException e) {
            throw new RakamException("File not found", HttpResponseStatus.NOT_FOUND);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void delete(int project, String name) {
        File dir = new File(directory, project + File.separator + name);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException();
        }

        deleteDirectory(dir);
    }
}
