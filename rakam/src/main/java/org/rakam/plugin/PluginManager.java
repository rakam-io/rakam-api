package org.rakam.plugin;


import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.rakam.ui.util.HttpDownloadHelper;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PluginManager {
    private static final File pluginsDirectory = new File(new File(System.getProperty("user.dir")), "plugins");

    private static final int EXIT_CODE_OK = 0;
    private static final int EXIT_CODE_CMD_USAGE = 64;
    private static final int EXIT_CODE_IO_ERROR = 74;
    private static final int EXIT_CODE_ERROR = 70;
    private static final ImmutableSet<Object> BLACKLIST = ImmutableSet.builder()
            .add("rakam",
                    "rakam.bat",
                    "rakam.in.sh",
                    "plugin",
                    "plugin.bat",
                    "service.bat").build();
    private String url;
    private OutputMode outputMode;

    public PluginManager(String url, OutputMode outputMode) {
        this.url = url;
        this.outputMode = outputMode;
    }

    public static void main(String[] args) {
        try {
            Files.createDirectories(pluginsDirectory.toPath());
        } catch (IOException e) {
            displayHelp("Unable to create plugins dir: ");
            System.exit(EXIT_CODE_ERROR);
        }

        String url = null;
        OutputMode outputMode = OutputMode.DEFAULT;
        String pluginName = null;
        int action = ACTION.NONE;

        if (args.length < 1) {
            displayHelp(null);
        }

        try {
            for (int c = 0; c < args.length; c++) {
                String command = args[c];
                switch (command) {
                    case "-u":
                    case "--url":
                        // deprecated versions:
                    case "url":
                    case "-url":
                        url = getCommandValue(args, ++c, "--url");
                        // Until update is supported, then supplying a URL implies installing
                        // By specifying this action, we also avoid silently failing without
                        //  dubious checks.
                        action = ACTION.INSTALL;
                        break;
                    case "-v":
                    case "--verbose":
                        // deprecated versions:
                    case "verbose":
                    case "-verbose":
                        outputMode = OutputMode.VERBOSE;
                        break;
                    case "-s":
                    case "--silent":
                        // deprecated versions:
                    case "silent":
                    case "-silent":
                        outputMode = OutputMode.SILENT;
                        break;
                    case "-i":
                    case "--install":
                        // deprecated versions:
                    case "install":
                    case "-install":
                        pluginName = getCommandValue(args, ++c, "--install");
                        action = ACTION.INSTALL;
                        break;
                    case "-r":
                    case "--remove":
                        // deprecated versions:
                    case "remove":
                    case "-remove":
                        pluginName = getCommandValue(args, ++c, "--remove");
                        action = ACTION.REMOVE;
                        break;
                    case "-l":
                    case "--list":
                        action = ACTION.LIST;
                        break;
                    case "-h":
                    case "--help":
                        displayHelp(null);
                        break;
                    default:
                        displayHelp("Command [" + command + "] unknown.");
                        // Unknown command. We break...
                        System.exit(EXIT_CODE_CMD_USAGE);
                }
            }
        } catch (Throwable e) {
            displayHelp("Error while parsing options: " + e.getClass().getSimpleName() +
                    ": " + e.getMessage());
            System.exit(EXIT_CODE_CMD_USAGE);
        }

        if (action > ACTION.NONE) {
            int exitCode = EXIT_CODE_ERROR; // we fail unless it's reset
            PluginManager pluginManager = new PluginManager(url, outputMode);
            switch (action) {
                case ACTION.INSTALL:
                    try {
                        pluginManager.log("-> Installing " + Strings.nullToEmpty(pluginName) + "...");
                        pluginManager.downloadAndExtract(pluginName);
                        exitCode = EXIT_CODE_OK;
                    } catch (IOException e) {
                        exitCode = EXIT_CODE_IO_ERROR;
                        pluginManager.log("Failed to install " + pluginName + ", reason: " + e.getMessage());
                    } catch (Throwable e) {
                        exitCode = EXIT_CODE_ERROR;
                        displayHelp("Error while installing plugin, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;
                case ACTION.REMOVE:
                    try {
                        pluginManager.log("-> Removing " + Strings.nullToEmpty(pluginName) + "...");
//                        pluginManager.removePlugin(pluginName);
                        exitCode = EXIT_CODE_OK;
                    } catch (IllegalArgumentException e) {
                        exitCode = EXIT_CODE_CMD_USAGE;
                        pluginManager.log("Failed to remove " + pluginName + ", reason: " + e.getMessage());
//                    } catch (IOException e) {
//                        exitCode = EXIT_CODE_IO_ERROR;
//                        pluginManager.log("Failed to remove " + pluginName + ", reason: " + e.getMessage());
                    } catch (Throwable e) {
                        exitCode = EXIT_CODE_ERROR;
                        displayHelp("Error while removing plugin, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;
                case ACTION.LIST:
                    try {
                        pluginManager.listInstalledPlugins();
                        exitCode = EXIT_CODE_OK;
                    } catch (Throwable e) {
                        displayHelp("Error while listing plugins, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;

                default:
                    pluginManager.log("Unknown Action [" + action + "]");
                    exitCode = EXIT_CODE_ERROR;

            }
            System.exit(exitCode); // exit here!
        }
    }

    /**
     * Get the value for the {@code flag} at the specified {@code arg} of the command line {@code args}.
     * <p/>
     * This is useful to avoid having to check for multiple forms of unset (e.g., "   " versus "" versus {@code null}).
     *
     * @param args Incoming command line arguments.
     * @param arg  Expected argument containing the value.
     * @param flag The flag whose value is being retrieved.
     * @return Never {@code null}. The trimmed value.
     * @throws NullPointerException           if {@code args} is {@code null}.
     * @throws ArrayIndexOutOfBoundsException if {@code arg} is negative.
     * @throws IllegalStateException          if {@code arg} is &gt;= {@code args.length}.
     * @throws IllegalArgumentException       if the value evaluates to blank ({@code null} or only whitespace)
     */
    private static String getCommandValue(String[] args, int arg, String flag) {
        if (arg >= args.length) {
            throw new IllegalStateException("missing value for " + flag + ". Usage: " + flag + " [value]");
        }

        // avoid having to interpret multiple forms of unset
        String trimmedValue = Strings.emptyToNull(args[arg].trim());

        // If we had a value that is blank, then fail immediately
        if (trimmedValue == null) {
            throw new IllegalArgumentException(
                    "value for " + flag + "('" + args[arg] + "') must be set. Usage: " + flag + " [value]");
        }

        return trimmedValue;
    }

    private static void displayHelp(String message) {
        SysOut.println("Usage:");
        SysOut.println("    -u, --url     [plugin location]   : Set exact URL to download the plugin from");
        SysOut.println("    -i, --install [plugin name]       : Downloads and installs listed plugins [*]");
        SysOut.println("    -t, --timeout [duration]          : Timeout setting: 30s, 1m, 1h... (infinite by default)");
        SysOut.println("    -r, --remove  [plugin name]       : Removes listed plugins");
        SysOut.println("    -l, --list                        : List installed plugins");
        SysOut.println("    -v, --verbose                     : Prints verbose messages");
        SysOut.println("    -s, --silent                      : Run in silent mode");
        SysOut.println("    -h, --help                        : Prints this help message");
        SysOut.newline();
        SysOut.println(" [*] Plugin name could be:");
        SysOut.println("     groupId/artifactId/version   for community plugins (download from maven central or oss sonatype)");
        SysOut.println("     username/repository          for site plugins (download from github master)");

        if (message != null) {
            SysOut.newline();
            SysOut.println("Message:");
            SysOut.println("   " + message);
        }
    }

//    public void removePlugin(String name) throws IOException {
//        if (name == null) {
//            throw new IllegalArgumentException("plugin name must be supplied with --remove [name].");
//        }
//        PluginHandle pluginHandle = PluginHandle.parse(name);
//        boolean removed = false;
//
//        checkForForbiddenName(pluginHandle.name);
//        Path pluginToDelete = pluginHandle.extractedDir(environment);
//        if (Files.exists(pluginToDelete)) {
//            debug("Removing: " + pluginToDelete);
//            try {
//                IOUtils.rm(pluginToDelete);
//            } catch (IOException ex){
//                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
//                        pluginToDelete.toString(), ex);
//            }
//            removed = true;
//        }
//        pluginToDelete = pluginHandle.distroFile(environment);
//        if (Files.exists(pluginToDelete)) {
//            debug("Removing: " + pluginToDelete);
//            try {
//                Files.delete(pluginToDelete);
//            } catch (Exception ex) {
//                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
//                        pluginToDelete.toString(), ex);
//            }
//            removed = true;
//        }
//        Path binLocation = pluginHandle.binDir(environment);
//        if (Files.exists(binLocation)) {
//            debug("Removing: " + binLocation);
//            try {
//                IOUtils.rm(binLocation);
//            } catch (IOException ex){
//                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
//                        binLocation.toString(), ex);
//            }
//            removed = true;
//        }
//
//        if (removed) {
//            log("Removed " + name);
//        } else {
//            log("Plugin " + name + " not found. Run plugin --list to get list of installed plugins.");
//        }
//    }
//
//    private static void checkForForbiddenName(String name) {
//        if (!hasLength(name) || BLACKLIST.contains(name.toLowerCase(Locale.ROOT))) {
//            throw new IllegalArgumentException("Illegal plugin name: " + name);
//        }
//    }

    public void downloadAndExtract(String name) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("plugin name must be supplied with --install [name].");
        }
        HttpDownloadHelper downloadHelper = new HttpDownloadHelper();
        boolean downloaded = false;
        HttpDownloadHelper.DownloadProgress progress;
        if (outputMode == OutputMode.SILENT) {
            progress = new HttpDownloadHelper.NullProgress();
        } else {
            progress = new HttpDownloadHelper.VerboseProgress(SysOut.getOut());
        }

        if (!Files.isWritable(pluginsDirectory.toPath())) {
            throw new IOException("plugin directory " + pluginsDirectory.getAbsolutePath() + " is read only");
        }

        PluginHandle pluginHandle = PluginHandle.parse(name);
//        checkForForbiddenName(pluginHandle.name);

//        Path pluginFile = pluginHandle.distroFile(environment);
//        final Path extractLocation = pluginHandle.extractedDir(environment);
//        if (Files.exists(extractLocation)) {
//            throw new IOException("plugin directory " + extractLocation.toAbsolutePath() + " already exists. To update the plugin, uninstall it first using --remove " + name + " command");
//        }

        // first, try directly from the URL provided
        Path pluginFile = null;
        if (url != null) {
            URL pluginUrl = new URL(url);
            log("Trying " + pluginUrl.toExternalForm() + "...");
            try {
                downloadHelper.download(pluginUrl, pluginFile, progress);
                downloaded = true;
            } catch (Exception e) {
                // ignore
            }
        }

        if (!downloaded) {
            // We try all possible locations
            for (URL url : pluginHandle.urls()) {
                log("Trying " + url.toExternalForm() + "...");
                try {
                    downloadHelper.download(url, pluginFile, progress);
                    downloaded = true;
                    break;
                } catch (Exception e) {
                }
            }
        }

        if (!downloaded) {
            throw new IOException("failed to download out of all possible locations..., use --verbose to get detailed information");
        }

    }

    public Path[] getListInstalledPlugins() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory.toPath())) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }

    public void listInstalledPlugins() throws IOException {
        Path[] plugins = getListInstalledPlugins();
        log("Installed plugins in " + pluginsDirectory.toPath().toAbsolutePath() + ":");
        if (plugins == null || plugins.length == 0) {
            log("    - No plugin detected");
        } else {
            for (int i = 0; i < plugins.length; i++) {
                log("    - " + plugins[i].getFileName());
            }
        }
    }

    private void debug(String line) {
        if (outputMode == OutputMode.VERBOSE) SysOut.println(line);
    }

    private void log(String line) {
        if (outputMode != OutputMode.SILENT) SysOut.println(line);
    }

    public enum OutputMode {
        DEFAULT, SILENT, VERBOSE
    }

    public static final class ACTION {
        public static final int NONE = 0;
        public static final int INSTALL = 1;
        public static final int REMOVE = 2;
        public static final int LIST = 3;
    }

    static class SysOut {

        public static void newline() {
            System.out.println();
        }

        public static void println(String msg) {
            System.out.println(msg);
        }

        public static PrintStream getOut() {
            return System.out;
        }
    }

    /**
     * Helper class to extract properly user name, repository name, version and plugin name
     * from plugin name given by a user.
     */
    static class PluginHandle {

        private final String name;
        private final String version;
        private final String user;
        private final String repo;

        PluginHandle(String name, String version, String user, String repo) {
            this.name = name;
            this.version = version;
            this.user = user;
            this.repo = repo;
        }

        private static void addUrl(List<URL> urls, String url) {
            try {
                urls.add(new URL(url));
            } catch (MalformedURLException e) {
                // We simply ignore malformed URL
            }
        }

        static PluginHandle parse(String name) {
            String[] elements = name.split("/");
            // We first consider the simplest form: pluginname
            String repo = elements[0];
            String user = null;
            String version = null;

            // We consider the form: username/pluginname
            if (elements.length > 1) {
                user = elements[0];
                repo = elements[1];

                // We consider the form: username/pluginname/version
                if (elements.length > 2) {
                    version = elements[2];
                }
            }

            if (repo.startsWith("rakam-")) {
                // remove rakam- prefix
                String endname = repo.substring("rakam-".length());
                return new PluginHandle(endname, version, user, repo);
            }

            return new PluginHandle(repo, version, user, repo);
        }

        List<URL> urls() {
            List<URL> urls = new ArrayList<>();
            if (version != null) {
                // Maven central repository
                addUrl(urls, "http://search.maven.org/remotecontent?filepath=" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Sonatype repository
                addUrl(urls, "https://oss.sonatype.org/service/local/repositories/releases/content/" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Github repository
                addUrl(urls, "https://github.com/" + user + "/" + repo + "/archive/" + version + ".zip");
            }
            // Github repository for master branch (assume site)
            addUrl(urls, "https://github.com/" + user + "/" + repo + "/archive/master.zip");
            return urls;
        }
    }

}