package de.gsi.chart.samples.utils.css;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javafx.beans.value.ObservableValue;

/**
 * URL Handler to allow loading css from StringProperties instead of files.
 * Must be called "Handler" in order for java.net.URL to be able to find us.
 * Created by James X. Nelson (james@wetheinter.net) on 8/21/16.
 * @see <a href="https://stackoverflow.com/questions/24704515/in-javafx-8-can-i-provide-a-stylesheet-from-a-string">Stackoverflow: stylesheet from string</a>
 */
public class Handler extends URLStreamHandler {
    private static final String URL_HANDLER_PROPERTY = "java.protocol.handler.pkgs";
    private static final Map<String, ObservableValue<String>> dynamicFiles = new HashMap<>();

    static {
        // Ensure that we are registered as a url protocol handler for css:/path css files.
        String was = System.getProperty(URL_HANDLER_PROPERTY, "");
        System.setProperty(URL_HANDLER_PROPERTY,
                Handler.class.getPackage().getName().replace(".css", "") + (was.isEmpty() ? "" : "|" + was));
    }

    public static void registerStyleSheet(String path, ObservableValue<String> contents) {
        dynamicFiles.put(path, contents);
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        final String path = u.getPath();
        final ObservableValue<String> file = dynamicFiles.get(path);
        if (file == null) {
            throw new IOException("No such css registered");
        }
        return new StringURLConnection(u, file.getValue());
    }

    private static class StringURLConnection extends URLConnection {
        private final String contents;

        public StringURLConnection(URL url, String contents) {
            super(url);
            this.contents = contents;
        }

        @Override
        public void connect() throws IOException {
            /* nothing to do here */ }

            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8));
            }
    }
}
