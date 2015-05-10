package org.rakam.bootstrap;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.rakam.plugin.RakamModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.artifact.Artifact;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 01:35.
 */
public abstract class ModuleInstaller {
    final static Logger LOGGER = LoggerFactory.getLogger(ModuleInstaller.class);


    private static final List<String> HIDDEN_CLASSES = com.google.common.collect.ImmutableList.<String>builder()
            .add("org.slf4j")
            .build();

    private static final com.google.common.collect.ImmutableList<String> PARENT_FIRST_CLASSES = com.google.common.collect.ImmutableList.<String>builder()
            .add("javax.inject")
            .add("javax.annotation")
            .add("java.")
            .build();

    private final ArtifactResolver resolver;

    public ModuleInstaller() {
        this.resolver = new ArtifactResolver(ArtifactResolver.USER_LOCAL_REPO, ArtifactResolver.MAVEN_CENTRAL_URI);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return com.google.common.collect.ImmutableList.copyOf(files);
            }
        }
        return com.google.common.collect.ImmutableList.of();
    }

    public void loadPlugins() {
        try {
            loadPlugin("rakam-ui/pom.xml");
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        return createClassLoader(artifacts, pomFile.getPath());
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws Exception
    {
        LOGGER.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            LOGGER.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
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
        return buildClassLoaderFromCoordinates(plugin);
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
        LOGGER.debug("Classpath for %s:", name);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            LOGGER.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, HIDDEN_CLASSES, PARENT_FIRST_CLASSES);
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    public void loadPlugin(String plugin)
            throws Exception
    {
        LOGGER.info("-- Loading plugin {} --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(plugin);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }
        LOGGER.info("-- Finished loading plugin {} --", plugin);
    }

    private void loadPlugin(URLClassLoader pluginClassLoader)
            throws Exception
    {
        ServiceLoader<RakamModule> serviceLoader = ServiceLoader.load(RakamModule.class, pluginClassLoader);
        List<RakamModule> plugins = com.google.common.collect.ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            LOGGER.warn("No service providers of type %s", RakamModule.class.getName());
        }

        for (RakamModule plugin : plugins) {
            LOGGER.info("Installing {}", plugin.getClass().getName());
            visit(plugin);
        }
    }

    public abstract void visit(RakamModule module);
}
