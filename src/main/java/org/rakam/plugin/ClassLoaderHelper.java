package org.rakam.plugin;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by buremba on 26/05/14.
 */
final class ClassLoaderHelper
{
    static ClassLoader buildClassLoader(
            List<File> directories, boolean includeSubDirs)
    {
        return buildClassLoader(
                directories, includeSubDirs, Thread.currentThread().getContextClassLoader());
    }

    static ClassLoader buildClassLoader(
            List<File> directories, boolean includeSubDirs, ClassLoader parent)
    {
        List<URL> allJars = new ArrayList<URL>();
        // Find all Jars in each directory
        for (File dir: directories)
        {
            fillJarsList(allJars, dir, includeSubDirs);
        }
        return new URLClassLoader(allJars.toArray(new URL[allJars.size()]), parent);
    }

    static private void fillJarsList(List<URL> jars, File dir, boolean includeSubDirs)
    {
        try
        {
            for (File jar: dir.listFiles(_jarsFilter))
            {
                jars.add(jar.toURI().toURL());
            }

            if (includeSubDirs)
            {
                for (File subdir: dir.listFiles(_dirsFilter))
                {
                    fillJarsList(jars, subdir, true);
                }
            }
        }
        catch (Exception e)
        {
            // Should not happen
        }
    }

    static final private FileFilter _jarsFilter = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return false;
        }
    };
    static final private FileFilter _dirsFilter = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return false;
        }
    };

}
