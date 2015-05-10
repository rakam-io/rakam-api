package org.rakam;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by buremba <Burak Emre Kabakcı> on 09/05/15 17:28.
 */
public class HttpDownloadHelper {

    private boolean useTimestamp = false;
    private boolean skipExisting = false;

    public boolean download(URL source, Path dest, DownloadProgress progress) throws Exception {
        if (Files.exists(dest) && skipExisting) {
            return true;
        }

        //don't do any progress, unless asked
        if (progress == null) {
            progress = new NullProgress();
        }

        //set the timestamp to the file date.
        long timestamp = 0;

        boolean hasTimestamp = false;
        if (useTimestamp && Files.exists(dest) ) {
            timestamp = Files.getLastModifiedTime(dest).toMillis();
            hasTimestamp = true;
        }

        GetThread getThread = new GetThread(source, dest, hasTimestamp, timestamp, progress);

        try {
            getThread.setDaemon(true);
            getThread.start();
            getThread.join(10000);

            if (getThread.isAlive()) {
                throw new RuntimeException("The GET operation took longer than " + 10000 + ", stopping it.");
            }
        }
        catch (InterruptedException ie) {
            return false;
        } finally {
            getThread.closeStreams();
        }

        return getThread.wasSuccessful();
    }


    /**
     * Interface implemented for reporting
     * progress of downloading.
     */
    public interface DownloadProgress {
        /**
         * begin a download
         */
        void beginDownload();

        /**
         * tick handler
         */
        void onTick();

        /**
         * end a download
         */
        void endDownload();
    }

    /**
     * do nothing with progress info
     */
    public static class NullProgress implements DownloadProgress {

        /**
         * begin a download
         */
        @Override
        public void beginDownload() {

        }

        /**
         * tick handler
         */
        @Override
        public void onTick() {
        }

        /**
         * end a download
         */
        @Override
        public void endDownload() {

        }
    }

    /**
     * verbose progress system prints to some output stream
     */
    public static class VerboseProgress implements DownloadProgress {
        private int dots = 0;
        // CheckStyle:VisibilityModifier OFF - bc
        PrintWriter writer;
        // CheckStyle:VisibilityModifier ON

        /**
         * Construct a verbose progress reporter.
         *
         * @param out the output stream.
         */
        public VerboseProgress(PrintStream out) {
            this.writer = new PrintWriter(out);
        }

        /**
         * Construct a verbose progress reporter.
         *
         * @param writer the output stream.
         */
        public VerboseProgress(PrintWriter writer) {
            this.writer = writer;
        }

        /**
         * begin a download
         */
        @Override
        public void beginDownload() {
            writer.print("Downloading ");
            dots = 0;
        }

        /**
         * tick handler
         */
        @Override
        public void onTick() {
            writer.print(".");
            if (dots++ > 50) {
                writer.flush();
                dots = 0;
            }
        }

        /**
         * end a download
         */
        @Override
        public void endDownload() {
            writer.println("DONE");
            writer.flush();
        }
    }

    private class GetThread extends Thread {

        private final URL source;
        private final Path dest;
        private final boolean hasTimestamp;
        private final long timestamp;
        private final DownloadProgress progress;

        private boolean success = false;
        private IOException ioexception = null;
        private InputStream is = null;
        private OutputStream os = null;
        private URLConnection connection;
        private int redirections = 0;

        GetThread(URL source, Path dest, boolean h, long t, DownloadProgress p) {
            this.source = source;
            this.dest = dest;
            hasTimestamp = h;
            timestamp = t;
            progress = p;
        }

        @Override
        public void run() {
            try {
                success = get();
            } catch (IOException ioex) {
                ioexception = ioex;
            }
        }

        private boolean get() throws IOException {

            connection = openConnection(source);

            if (connection == null) {
                return false;
            }

            boolean downloadSucceeded = downloadFile();

            //if (and only if) the use file time option is set, then
            //the saved file now has its timestamp set to that of the
            //downloaded file
            if (downloadSucceeded && useTimestamp) {
                updateTimeStamp();
            }

            return downloadSucceeded;
        }


        private boolean redirectionAllowed(URL aSource, URL aDest) throws IOException {
            // Argh, github does this...
//            if (!(aSource.getProtocol().equals(aDest.getProtocol()) || ("http"
//                    .equals(aSource.getProtocol()) && "https".equals(aDest
//                    .getProtocol())))) {
//                String message = "Redirection detected from "
//                        + aSource.getProtocol() + " to " + aDest.getProtocol()
//                        + ". Protocol switch unsafe, not allowed.";
//                throw new IOException(message);
//            }

            redirections++;
            if (redirections > 5) {
                String message = "More than " + 5 + " times redirected, giving up";
                throw new IOException(message);
            }


            return true;
        }

        private URLConnection openConnection(URL aSource) throws IOException {

            // set up the URL connection
            URLConnection connection = aSource.openConnection();
            // modify the headers
            // NB: things like user authentication could go in here too.
            if (hasTimestamp) {
                connection.setIfModifiedSince(timestamp);
            }

            if (connection instanceof HttpURLConnection) {
                ((HttpURLConnection) connection).setInstanceFollowRedirects(false);
                ((HttpURLConnection) connection).setUseCaches(true);
                ((HttpURLConnection) connection).setConnectTimeout(5000);
            }
            connection.setRequestProperty("User-Agent", "elasticsearch-plugin-manager");

            // connect to the remote site (may take some time)
            connection.connect();

            // First check on a 301 / 302 (moved) response (HTTP only)
            if (connection instanceof HttpURLConnection) {
                HttpURLConnection httpConnection = (HttpURLConnection) connection;
                int responseCode = httpConnection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_MOVED_PERM ||
                        responseCode == HttpURLConnection.HTTP_MOVED_TEMP ||
                        responseCode == HttpURLConnection.HTTP_SEE_OTHER) {
                    String newLocation = httpConnection.getHeaderField("Location");
                    String message = aSource
                            + (responseCode == HttpURLConnection.HTTP_MOVED_PERM ? " permanently"
                            : "") + " moved to " + newLocation;
                    URL newURL = new URL(newLocation);
                    if (!redirectionAllowed(aSource, newURL)) {
                        return null;
                    }
                    return openConnection(newURL);
                }
                // next test for a 304 result (HTTP only)
                long lastModified = httpConnection.getLastModified();
                if (responseCode == HttpURLConnection.HTTP_NOT_MODIFIED
                        || (lastModified != 0 && hasTimestamp && timestamp >= lastModified)) {
                    // not modified so no file download. just return
                    // instead and trace out something so the user
                    // doesn't think that the download happened when it
                    // didn't
                    return null;
                }
                // test for 401 result (HTTP only)
                if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                    String message = "HTTP Authorization failure";
                    throw new IOException(message);
                }
            }

            //REVISIT: at this point even non HTTP connections may
            //support the if-modified-since behaviour -we just check
            //the date of the content and skip the write if it is not
            //newer. Some protocols (FTP) don't include dates, of
            //course.
            return connection;
        }

        private boolean downloadFile() throws FileNotFoundException, IOException {
            IOException lastEx = null;
            for (int i = 0; i < 3; i++) {
                // this three attempt trick is to get round quirks in different
                // Java implementations. Some of them take a few goes to bind
                // property; we ignore the first couple of such failures.
                try {
                    is = connection.getInputStream();
                    break;
                } catch (IOException ex) {
                    lastEx = ex;
                }
            }
            if (is == null) {
                throw new IOException("Can't get " + source + " to " + dest, lastEx);
            }

            os = Files.newOutputStream(dest);
            progress.beginDownload();
            boolean finished = false;
            try {
                byte[] buffer = new byte[1024 * 100];
                int length;
                while (!isInterrupted() && (length = is.read(buffer)) >= 0) {
                    os.write(buffer, 0, length);
                    progress.onTick();
                }
                finished = !isInterrupted();
            } finally {
                if (!finished) {
                    // we have started to (over)write dest, but failed.
                    // Try to delete the garbage we'd otherwise leave
                    // behind.
                    closeWhileHandlingException(Arrays.asList(os, is));
                    deleteFilesIgnoringExceptions(Arrays.asList(dest));
                } else {
                    os.close();
                    is.close();
                }
            }
            progress.endDownload();
            return true;
        }




        public void deleteFilesIgnoringExceptions(Collection<? extends Path> files) {
            for (Path name : files) {
                if (name != null) {
                    try {
                        Files.delete(name);
                    } catch (Throwable ignored) {
                        // ignore
                    }
                }
            }
        }

        public void closeWhileHandlingException(Iterable<? extends Closeable> objects) {
            for (Closeable object : objects) {
                try {
                    if (object != null) {
                        object.close();
                    }
                } catch (Throwable t) {
                }
            }
        }


        private void updateTimeStamp() throws IOException {
            long remoteTimestamp = connection.getLastModified();
            if (remoteTimestamp != 0) {
                Files.setLastModifiedTime(dest, FileTime.fromMillis(remoteTimestamp));
            }
        }

        /**
         * Has the download completed successfully?
         * <p/>
         * <p>Re-throws any exception caught during executaion.</p>
         */
        boolean wasSuccessful() throws IOException {
            if (ioexception != null) {
                throw ioexception;
            }
            return success;
        }

        /**
         * Closes streams, interrupts the download, may delete the
         * output file.
         */
        void closeStreams() throws IOException {
            interrupt();
            if (success) {
                is.close();
                os.close();
            } else {
                closeWhileHandlingException(Arrays.asList(is, os));
                if (dest != null && Files.exists(dest)) {
                    deleteFilesIgnoringExceptions(Arrays.asList(dest));
                }
            }
        }
    }
}
