package org.rakam;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.rakam.config.PluginConfig;
import org.rakam.plugin.RakamModule;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class PluginInstaller
{
    final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServiceStarter.class);

    private static final List<String> HIDDEN_CLASSES = ImmutableList.<String>builder()
            .add("org.slf4j")
            .build();

    private static final ImmutableList<String> PARENT_FIRST_CLASSES = ImmutableList.<String>builder()
            .add("com.facebook.presto")
            .add("com.fasterxml.jackson")
            .add("io.airlift.slice")
            .add("javax.inject")
            .add("javax.annotation")
            .add("java.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);

    private final Consumer<RakamModule> consumer;
    private final ArtifactResolver resolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final PluginConfig config;

    @Inject
    public PluginInstaller(PluginConfig config, Consumer<RakamModule> consumer)
    {
        checkNotNull(consumer, "consumer is null");
        checkNotNull(config, "config is null");

        this.consumer = consumer;
        this.config = config;
        this.resolver = new ArtifactResolver(ArtifactResolver.USER_LOCAL_REPO, ArtifactResolver.MAVEN_CENTRAL_URI);
    }

    public boolean arePluginsLoaded()
    {
        return pluginsLoaded.get();
    }

    public void loadPlugins()
            throws Exception
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

//        for (File file : listFiles(installedPluginsDir)) {
//            if (file.isDirectory()) {
//                loadPlugin(file.getAbsolutePath());
//            }
//        }

        for (String plugin : config.getPlugins()) {
            loadPlugin(plugin);
        }

        pluginsLoaded.set(true);
    }

    private void loadPlugin(String plugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader;

        if(plugin.startsWith("file://")) {
           pluginClassLoader = buildClassLoader(plugin.substring("file://".length()));
        }else
        if(plugin.startsWith("github://")) {
            HttpDownloadHelper downloadHelper = new HttpDownloadHelper();
            String[] split = plugin.substring("github://".length()).split(":");
            URL url;
            String[] repo = split[0].split("/");
            String name;
            if (split.length > 1) {
                url = new URL("https://github.com/" + repo[0] + "/" + repo[1] + "/archive/" + split[1] + ".zip");
                name = repo[1]+"-"+ split[1];
            } else {
                url = new URL("https://github.com/" + repo[0] + "/" + repo[1] + "/archive/master.zip");
                name = repo[1]+"-master";
            }

            config.getPluginsDirectory().mkdirs();
            File zipFile = new File(config.getPluginsDirectory(), repo[1]+".zip");
            File folder = new File(config.getPluginsDirectory(), name);

            if(!folder.exists()) {
                if (!zipFile.exists()) {
                    boolean download = downloadHelper.download(url, zipFile.toPath(),
                            new HttpDownloadHelper.DownloadProgress() {
                                @Override
                                public void beginDownload() {
                                    LOGGER.info("Starting to download {} from {}.", plugin, url);
                                }

                                @Override
                                public void onTick() {
                                }

                                @Override
                                public void endDownload() {
                                    LOGGER.info("Downloaded {} from {}.", plugin, url);
                                }
                            });
                    if (!download) {
                        LOGGER.warn("Couldn't download {} from {}.", plugin, url);
                        return;
                    }
                }
            }
            if(!folder.exists()) {
                extractFolder(zipFile, folder.getParentFile());
                zipFile.delete();
            }
            String pomFile = folder.getAbsolutePath() + File.separator + "pom.xml";
            pluginClassLoader = buildClassLoader(pomFile);
            InvocationRequest request = new DefaultInvocationRequest();
            request.setPomFile( new File(pomFile) );
            request.setGoals( Arrays.asList( "clean", "install" ) );

            DefaultInvoker invoker = new DefaultInvoker();
//            invoker.setMavenHome();
            InvocationResult execute = invoker.execute(request);
            if(execute.getExecutionException() != null) {
                throw execute.getExecutionException();
            }

        } else {
            throw new UnsupportedOperationException();
        }
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void extractFolder(File file, File extractFolder) throws IOException {
            int BUFFER = 2048;

            ZipFile zip = new ZipFile(file);

            extractFolder.mkdir();
            Enumeration zipFileEntries = zip.entries();

            // Process each entry
            while (zipFileEntries.hasMoreElements())
            {
                // grab a zip file entry
                ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
                String currentEntry = entry.getName();

                File destFile = new File(extractFolder, currentEntry);
                //destFile = new File(newPath, destFile.getName());
                File destinationParent = destFile.getParentFile();

                // create the parent directory structure if needed
                destinationParent.mkdirs();

                if (!entry.isDirectory())
                {
                    BufferedInputStream is = new BufferedInputStream(zip
                            .getInputStream(entry));
                    int currentByte;
                    // establish buffer for writing file
                    byte data[] = new byte[BUFFER];

                    // write the current file to disk
                    FileOutputStream fos = new FileOutputStream(destFile);
                    BufferedOutputStream dest = new BufferedOutputStream(fos,
                            BUFFER);

                    // read and write until last byte is encountered
                    while ((currentByte = is.read(data, 0, BUFFER)) != -1) {
                        dest.write(data, 0, currentByte);
                    }
                    dest.flush();
                    dest.close();
                    is.close();
                }


            }


    }

    private void loadPlugin(URLClassLoader pluginClassLoader)
            throws Exception
    {
        ServiceLoader<RakamModule> serviceLoader = ServiceLoader.load(RakamModule.class, pluginClassLoader);
        List<RakamModule> plugins = ImmutableList.copyOf(serviceLoader);
        if (plugins.isEmpty()) {
            log.warn("No service providers of type %s", RakamModule.class.getName());
        }


        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();

        try {
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
            method.setAccessible(true);
            for (URL url : pluginClassLoader.getURLs()) {
                method.invoke(sysloader, new Object[]{url});
            }
        } catch (Throwable t) {
            throw new IOException("Error, could not add URL to system classloader");
        }

        for (RakamModule plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());

            consumer.accept(plugin);
        }
    }

    private URLClassLoader buildClassLoader(String plugin)
            throws Exception
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file);
        }
        if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        if(file.isFile()) {
//            return new URLClassLoader(new URL[]{new URL("file://"+plugin)});
            return createClassLoader(Arrays.asList(new URL("file://"+plugin)));
        }
        return buildClassLoaderFromCoordinates(plugin);
    }

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        return createClassLoader(artifacts, pomFile.getPath());
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws Exception
    {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromCoordinates(String coordinates)
            throws Exception
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString());
    }

    private URLClassLoader createClassLoader(List<Artifact> artifacts, String name)
            throws IOException
    {
        log.debug("Classpath for %s:", name);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, HIDDEN_CLASSES, PARENT_FIRST_CLASSES);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    public static class ThreadContextClassLoader
            implements Closeable
    {
        private final ClassLoader originalThreadContextClassLoader;

        public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader)
        {
            this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
        }

        @Override
        public void close()
        {
            Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
        }
    }
}
