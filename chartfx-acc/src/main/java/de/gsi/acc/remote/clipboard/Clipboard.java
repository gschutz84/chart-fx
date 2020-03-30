package de.gsi.acc.remote.clipboard;

import static de.gsi.acc.remote.BasicRestRoles.ANYONE;
import static de.gsi.acc.remote.RestServer.prefixPath;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.zip.Deflater;

import javafx.beans.property.IntegerProperty;
import javafx.beans.property.ReadOnlyIntegerProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.scene.SnapshotParameters;
import javafx.scene.image.Image;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.Region;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.gsi.acc.remote.BasicRestRoles;
import de.gsi.acc.remote.MimeType;
import de.gsi.acc.remote.RestCommonThreadPool;
import de.gsi.acc.remote.RestServer;
import de.gsi.acc.remote.util.CombinedHandler;
import de.gsi.acc.remote.util.MessageBundle;
import de.gsi.chart.utils.FXUtils;
import de.gsi.chart.utils.PaletteQuantizer;
import de.gsi.chart.utils.WritableImageCache;
import de.gsi.chart.utils.WriteFxImage;
import de.gsi.dataset.event.EventListener;
import de.gsi.dataset.event.EventRateLimiter;
import de.gsi.dataset.event.EventSource;
import de.gsi.dataset.event.UpdateEvent;
import de.gsi.dataset.remote.Data;
import de.gsi.dataset.remote.DataContainer;
import de.gsi.dataset.serializer.spi.GenericsHelper;
import de.gsi.dataset.utils.ByteArrayCache;
import de.gsi.dataset.utils.Cache;
import de.gsi.dataset.utils.Cache.CacheBuilder;
import de.gsi.math.TMath;

import ar.com.hjg.pngj.FilterType;
import io.javalin.core.security.Role;
import io.javalin.http.Handler;
import io.javalin.http.sse.SseClient;

/**
 * Basic implementation of a Restfull image clipboard
 * 
 * RestServer property are mainly controlled via {@link RestServer}
 * 
 * N.B. The '/upload' endpoint access requires write privileges. By default, non
 * logged-in users are mapped to 'anonymous' that by-default have READ_WRITE
 * access roles. This can be reconfigured in the default Password file and/or
 * another custom {@link de.gsi.acc.remote.user.RestUserHandler }
 * implementation.
 * 
 * @author rstein
 *
 */
public class Clipboard implements EventSource, EventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(Clipboard.class);
    private static final int DEFAULT_PALETTE_COLOR_COUNT = 16;
    private static final String TESTIMAGE = "PM5544_test_signal.png";
    private static final String DOT_PNG = ".png";
    private static final String QUERY_UPDATE_PERIOD = "updatePeriod";
    private static final String QUERY_LONG_POLLING = "longpolling";
    private static final String QUERY_SSE = "sse";
    private static final String QUERY_LAST_UPDATE = "lastAccess.";
    private static final String CLIPBOARD_TAG = "status";
    private static final String CLIPBOARD_BASE = "/clipboard/";
    private static final String CLIPBOARD_IMG_BASE = CLIPBOARD_BASE + "images/";
    private static final String ENDPOINT_UPLOAD = "/upload";
    private static final String ENDPOINT_CLIPBOARD = "/clipboard/:" + CLIPBOARD_TAG;
    private static final String ENDPOINT_CLIPBOARD_IMG = CLIPBOARD_IMG_BASE + ":status";
    private static final String TEMPLATE_UPLOAD = "/velocity/clipboard/upload.vm";
    private static final String TEMPLATE_ALL_IMAGES = "/velocity/clipboard/all.vm";
    private static final String TEMPLATE_ONE_IMAGE_LONG_POLLING = "/velocity/clipboard/one_long.vm";
    private static final String TEMPLATE_ONE_IMAGE_SSE = "/velocity/clipboard/one_sse.vm";

    private static final String CACHE_LIMIT = "clipboardCacheLimit";
    private static final int CACHE_LIMIT_DEFAULT = 25;
    private static final String CACHE_TIME_OUT = "clipboardCacheTimeOut"; // [minutes]
    private static final int CACHE_TIME_OUT_DEFAULT = 60;

    // update source definitions
    private final transient AtomicBoolean autoNotification = new AtomicBoolean(true);
    private final transient List<EventListener> updateListeners = Collections.synchronizedList(new LinkedList<>());

    private final Lock clipboardLock = new ReentrantLock();
    private final Condition clipboardCondition = clipboardLock.newCondition();
    private final Cache<String, DataContainer> clipboardCache;
    private final SnapshotParameters snapshotParameters = new SnapshotParameters();
    private final Cache<String, String> userCounterCache = Cache.<String, String>builder().withTimeout(1, TimeUnit.MINUTES).build();
    private final IntegerProperty userCount = new SimpleIntegerProperty(this, "userCount", 0);
    private final IntegerProperty userCountSse = new SimpleIntegerProperty(this, "userCountSse", 0);
    private final String exportRoot;
    private final String exportName;
    private final String exportNameImage;
    private boolean useAlpha = true;
    private final Region regionToCapture;
    private PaletteQuantizer userPalette = null;
    private final EventListener paletteUpdateListener = evt -> {
        if (evt.getPayLoad() instanceof Image) {
            RestCommonThreadPool.getCommonPool().execute(() -> userPalette = WriteFxImage.estimatePalette((Image) (evt.getPayLoad()), useAlpha, DEFAULT_PALETTE_COLOR_COUNT));
        }
    };
    private EventRateLimiter paletteUpdateRateLimiter = new EventRateLimiter(paletteUpdateListener, TimeUnit.SECONDS.toMillis(20));
    private final long maxUpdatePeriod;
    private final TimeUnit maxUpdatePeriodTimeUnit;

    private final WritableImageCache imageCache = new WritableImageCache();
    private final ByteArrayCache byteArrayCache = new ByteArrayCache();
    private final EventRateLimiter eventRateLimiter;

    private final AtomicInteger threadCount = new AtomicInteger(0);
    private final List<Double> captureDiffs = new ArrayList<>(250);
    private final List<Double> processingTotal = new ArrayList<>(250);
    private final List<Double> sizeTotal = new ArrayList<>(250);

    private final Runnable convertImage = () -> {
        final long start = System.nanoTime();
        final int width = (int) getRegionToCapture().getWidth();
        final int height = (int) getRegionToCapture().getHeight();
        if (threadCount.get() > 1 || width == 0 || height == 0) {
            return;
        }
        threadCount.incrementAndGet();

        final WritableImage imageCopyIn = imageCache.getImage(width, height);
        WritableImage imageCopyOut = null;
        try {
            imageCopyOut = FXUtils.runAndWait(() -> getRegionToCapture().snapshot(snapshotParameters, imageCopyIn));
        } catch (final Exception e) {
            LOGGER.atError().setCause(e).log("snapshotListener -> Node::snapshot(..)");
            threadCount.decrementAndGet();
            return;
        }
        captureDiffs.add(((System.nanoTime() - start) / 1e6));

        if (imageCopyOut == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(width).addArgument(height).log("snapshotListener - return image is null - requested '{}x{}'");
            }
            threadCount.decrementAndGet();
            return;
        }
        final long mid = System.nanoTime();
        final int size2 = WriteFxImage.getCompressedSizeBound(width, height, true);
        final byte[] rawByteBuffer = byteArrayCache.getArray(size2);
        final ByteBuffer imageBuffer = ByteBuffer.wrap(rawByteBuffer);
        // WriteFxImage.encodeAlt(imageCopyOut, imageBuffer, useAlpha, Deflater.BEST_SPEED, null);
        // WriteFxImage.encode(imageCopyOut, imageBuffer, useAlpha, Deflater.BEST_SPEED, FilterType.FILTER_NONE);

        updatePalette(imageCopyOut);
        WriteFxImage.encodePalette(imageCopyOut, imageBuffer, useAlpha, Deflater.BEST_SPEED, FilterType.FILTER_NONE, userPalette);
        sizeTotal.add(Double.valueOf(imageBuffer.limit()));
        imageCache.add(imageCopyIn);
        imageCache.add(imageCopyOut);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.atDebug().addArgument(getExportNameImage()).addArgument(getExportName()) //
                    .log("new image '{}' for export name '{}' generated -> notify listener");
        }
        final int maxUpdatePeriodMillis = (int) getMaxUpdatePeriodTimeUnit().toMillis(getMaxUpdatePeriod());
        addClipboardData(new DataContainer(getExportName(), "/clipboard/", maxUpdatePeriodMillis, new Data(getExportNameImage(), MimeType.PNG.toString(), imageBuffer.array(), imageBuffer.limit())));
        processingTotal.add(((System.nanoTime() - mid) / 1e6));

        printDiffs("capture", "ms", captureDiffs);
        printDiffs("processingTotal", "ms", processingTotal);
        printDiffs("sizeTotal", "bytes", sizeTotal);

        final int threads = threadCount.decrementAndGet();
        if (threads > 1) {
            LOGGER.atWarn().addArgument(threads).log("thread-pile-up = {}");
        }
    };

    private final Handler exportImageHandler = ctx -> {
        final long maxUpdateMillis = getMaxUpdatePeriodTimeUnit().toMillis(getMaxUpdatePeriod());
        final int maxUpdateRate = 1000 / (int) maxUpdateMillis;
        RestServer.applyRateLimit(ctx, 2 * maxUpdateRate, TimeUnit.SECONDS); // rate limit on query exportNameImage landing page
        final String requestedImage = ctx.pathParam(CLIPBOARD_TAG);
        DataContainer cbData = getClipboardCache().get(requestedImage);
        if (cbData == null) {
            // image does not exist
            ctx.res.sendError(404);
            return;
        }

        final boolean isLongPolling = ctx.queryParam(QUERY_LONG_POLLING) != null;
        final String identifier = ctx.req.getRemoteAddr(); // find perhaps a better metric
        userCounterCache.put(identifier, ctx.req.getProtocol());
        FXUtils.runFX(() -> userCount.set(userCounterCache.size()));

        Long sessionUpdate = (Long) ctx.sessionAttribute(QUERY_LAST_UPDATE + ctx.path());
        final long lastUpdate = sessionUpdate == null ? 0 : sessionUpdate.longValue();
        ctx.contentType(MimeType.PNG.toString());
        RestServer.suppressCaching(ctx);

        while (cbData.getTimeStampCreation() <= lastUpdate && isLongPolling /* && cbData.getMaxUpdatePeriod() > 0 */) {
            try {
                final long waitPeriod = Math.max(TimeUnit.SECONDS.toMillis(1), 4 * cbData.getUpdatePeriod());
                clipboardLock.lock();
                final boolean condition1 = !clipboardCondition.await(waitPeriod, TimeUnit.MILLISECONDS) && LOGGER.isInfoEnabled();
                if (condition1) {
                    LOGGER.atInfo().log("aborted a possibly too long long-polling await");
                }
            } catch (final Exception e) {
                LOGGER.atError().setCause(e).addArgument(requestedImage).log("waiting for new image '{}' to be updated");
            } finally {
                clipboardLock.unlock();
            }
            cbData = getClipboardCache().get(requestedImage);
            if (cbData == null) {
                // image does not exist
                return;
            }
        }

        if (cbData != null) {
            ctx.sessionAttribute(QUERY_LAST_UPDATE + ctx.path(), cbData.getTimeStampCreation());
            RestServer.writeBytesToContext(ctx, cbData.getDataByteArray(), cbData.getDataByteArraySize());
        }
    };

    private final Handler exportHandler = ctx -> {
        ctx.res.setContentType(MimeType.HTML.toString());
        RestServer.applyRateLimit(ctx, 10, TimeUnit.MINUTES); // rate limit on query exportName landing page
        RestServer.suppressCaching(ctx);
        final String landingPage = ctx.pathParam(CLIPBOARD_TAG);

        // check if landing page exists
        final Entry<String, DataContainer> matchingEntry = getClipboardCache().entrySet().stream().filter(kv -> kv.getValue().getExportName().equals(landingPage)).findFirst().orElse(null);
        if (matchingEntry == null) {
            // landing page does not (yet) exist
            ctx.res.sendError(404);
            return;
        }
        final DataContainer matchedClipboardData = matchingEntry.getValue();

        final String updatePeriodString = ctx.queryParam(QUERY_UPDATE_PERIOD, "1000");
        long updatePeriod = 500;
        if (updatePeriodString != null) {
            try {
                updatePeriod = Long.valueOf(updatePeriodString);
            } catch (final Exception e) {
                final String clientIp = ctx.req.getRemoteHost();
                LOGGER.atError().setCause(e).addArgument(updatePeriodString).addArgument(clientIp).log("could not parse 'updatePeriod'={} argument sent by client {}");
            }
        }
        updatePeriod = Math.max(getMaxUpdatePeriod(), updatePeriod);
        final Map<String, Object> model = MessageBundle.baseModel(ctx);
        model.put("indexRoot", getExportRoot());
        model.put(QUERY_UPDATE_PERIOD, updatePeriod);
        model.put("title", matchedClipboardData.getExportName());
        model.put("imageLanding", CLIPBOARD_BASE + matchedClipboardData.getExportName() + "?updatePeriod=" + matchedClipboardData.getUpdatePeriod());
        model.put("imageSource", CLIPBOARD_IMG_BASE + matchedClipboardData.getExportNameData());
        model.put(QUERY_LONG_POLLING, QUERY_LONG_POLLING);
        if (ctx.queryParam(QUERY_SSE) == null) {
            ctx.render(TEMPLATE_ONE_IMAGE_LONG_POLLING, model);
        } else {
            ctx.render(TEMPLATE_ONE_IMAGE_SSE, model);
        }
    };

    private final Handler uploadHandlerGet = ctx -> {
        final Map<String, Object> model = MessageBundle.baseModel(ctx);
        ctx.render(TEMPLATE_UPLOAD, model);
    };

    private final Handler uploadHandlerPost = ctx -> {
        final String exportName = ctx.formParam("clipboarExportName");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.atDebug().addArgument(exportName).log("received export name = '{}'");
        }

        ctx.uploadedFiles("clipboardData").forEach(file -> {
            LOGGER.atInfo().addArgument(file.getFilename()).log("upload received: '{}'");
            try {
                final byte[] fileData = file.getContent().readAllBytes();
                addClipboardData(new DataContainer(file.getFilename(), MimeType.BINARY.toString(), fileData, fileData.length));
            } catch (final IOException e) {
                LOGGER.atError().setCause(e).addArgument(file.getFilename()).log("error while reading test image from '{}'");
            }
        });
        ctx.redirect(this.getExportRoot());
        // alt: ctx.html("Upload complete")
    };

    private final Handler fetchAllImages = ctx -> {
        final Map<String, Object> model = MessageBundle.baseModel(ctx);
        model.put("images", getClipboardCache().values());
        ctx.render(TEMPLATE_ALL_IMAGES, model);
    };

    public Clipboard(final String exportRoot, final String exportName, final Region regionToCapture, final long maxUpdatePeriod, final TimeUnit maxUpdatePeriodTimeUnit, final boolean allowUploads) {
        this.exportRoot = exportRoot;
        this.exportName = exportName;
        exportNameImage = exportName + DOT_PNG;
        this.regionToCapture = regionToCapture;
        this.maxUpdatePeriod = maxUpdatePeriod;
        this.maxUpdatePeriodTimeUnit = maxUpdatePeriodTimeUnit;

        CacheBuilder<String, DataContainer> clipboardCacheBuilder = Cache.<String, DataContainer>builder().withLimit(getCacheLimit());
        if (getCacheTimeOut() > 0) {
            clipboardCacheBuilder.withTimeout(getCacheTimeOut(), getCacheTimeOutUnit());
        }
        final BiConsumer<String, DataContainer> cacheRecoverAction = (final String k, final DataContainer v) -> // too long for one line
                RestCommonThreadPool.getCommonScheduledPool().schedule(() -> v.getData().forEach(d -> byteArrayCache.add(d.getDataByteArray())), 200, TimeUnit.MILLISECONDS);
        clipboardCache = clipboardCacheBuilder.withPostListener(cacheRecoverAction).build();

        eventRateLimiter = new EventRateLimiter(evt -> RestCommonThreadPool.getCommonPool().execute(convertImage), maxUpdatePeriodTimeUnit.toMillis(maxUpdatePeriod));

        // add default routes
        Set<Role> accessRoles = Collections.singleton(ANYONE);
        RestServer.getInstance().get(exportRoot, new CombinedHandler(fetchAllImages), accessRoles);
        RestServer.getInstance().get(prefixPath(ENDPOINT_CLIPBOARD_IMG), new CombinedHandler(exportImageHandler), accessRoles);
        RestServer.getInstance().get(prefixPath(ENDPOINT_CLIPBOARD), new CombinedHandler(exportHandler), accessRoles);

        if (allowUploads) {
            RestServer.getInstance().get(exportRoot + ENDPOINT_UPLOAD, new CombinedHandler(uploadHandlerGet), Set.of(BasicRestRoles.ADMIN, BasicRestRoles.READ_WRITE));
            RestServer.getInstance().post(exportRoot + ENDPOINT_UPLOAD, uploadHandlerPost, Set.of(BasicRestRoles.ADMIN, BasicRestRoles.READ_WRITE));
        }
    }

    /**
     * Adds new Clipboard data (non-blocking) to the cache and notifies potential
     * listeners.
     * 
     * @param data
     */
    public void addClipboardData(@NotNull final DataContainer data) {
        RestCommonThreadPool.getCommonPool().execute(() -> {
            clipboardLock.lock();
            try {
                getClipboardCache().put(data.getExportNameData(), data);
                data.updateAccess();
                updateListener(CLIPBOARD_IMG_BASE + data.getExportNameData(), data.getTimeStampCreation());
                clipboardCondition.signalAll();
            } finally {
                clipboardLock.unlock();
            }
        });
    }

    public void addTestImageData() {
        try (InputStream in = Clipboard.class.getResourceAsStream(TESTIMAGE)) {
            byte[] fileContent = in.readAllBytes();
            addClipboardData(new DataContainer("test.png", MimeType.PNG.toString(), fileContent, fileContent.length));
        } catch (final IOException e) {
            final URL res = DataContainer.class.getResource(TESTIMAGE);
            LOGGER.atError().setCause(e).addArgument(res == null ? null : res.getPath()).log("error while reading test image from '{}'");
        }
    }

    @Override
    public AtomicBoolean autoNotification() {
        return autoNotification;
    }

    /**
     * 
     * @return the underlying clipbard data cache N.B. use preferably
     *         {@link #addClipboardData} for adding data since this does not block
     *         and also notifies listeners
     */
    public Cache<String, DataContainer> getClipboardCache() {
        return clipboardCache;
    }

    public String getExportName() {
        return exportName;
    }

    public String getExportNameImage() {
        return exportNameImage;
    }

    public URI getLocalURI() {
        return URI.create(new StringBuilder().append(RestServer.getLocalURI().toString()).append(prefixPath(getExportRoot())).toString());
    }

    public long getMaxUpdatePeriod() {
        return maxUpdatePeriod;
    }

    public TimeUnit getMaxUpdatePeriodTimeUnit() {
        return maxUpdatePeriodTimeUnit;
    }

    public EventRateLimiter getPaletteUpdateRateLimiter() {
        return paletteUpdateRateLimiter;
    }

    public URI getPublicURI() {
        return URI.create(RestServer.getPublicURI().toString() + prefixPath(getExportRoot()));
    }

    public Region getRegionToCapture() {
        return regionToCapture;
    }

    @Override
    public void handle(final UpdateEvent event) {
        eventRateLimiter.handle(event);
    }

    public void setPaletteUpdateRateLimiter(final long timeOut, final TimeUnit timeUnit) {
        paletteUpdateRateLimiter = new EventRateLimiter(paletteUpdateListener, timeUnit.toMillis(timeOut));
    }

    @Override
    public List<EventListener> updateEventListener() {
        return updateListeners;
    }

    public void updateListener(@NotNull final String eventSource, final long eventTimeStamp) {
        final Queue<SseClient> sseClients = RestServer.getEventClients(eventSource);
        FXUtils.runFX(() -> userCountSse.set(sseClients.size()));
        sseClients.forEach((final SseClient client) -> client.sendEvent(new StringBuilder().append("new '").append(eventSource).append("' @").append(eventTimeStamp).toString()));
    }

    public ReadOnlyIntegerProperty userCountProperty() {
        return userCount;
    }

    public ReadOnlyIntegerProperty userCountSseProperty() {
        return userCountSse;
    }

    protected String getExportRoot() {
        return exportRoot;
    }

    protected void updatePalette(Image imageCopyOut) {
        paletteUpdateRateLimiter.handle(new UpdateEvent(this, "update palette", WriteFxImage.clone(imageCopyOut)));
    }
    public static int getCacheLimit() {
        final String property = System.getProperty(CACHE_LIMIT, Integer.toString(CACHE_LIMIT_DEFAULT));
        try {
            return Integer.parseInt(property);
        } catch (final NumberFormatException e) {
            LOGGER.atError().addArgument(CACHE_LIMIT).addArgument(property).addArgument(CACHE_LIMIT_DEFAULT).log("could not parse {}='{}' return default limit {}");
            return CACHE_LIMIT_DEFAULT;
        }
    }
    public static int getCacheTimeOut() {
        final String property = System.getProperty(CACHE_TIME_OUT, Integer.toString(CACHE_TIME_OUT_DEFAULT));
        try {
            return Integer.parseInt(property);
        } catch (final NumberFormatException e) {
            LOGGER.atError().addArgument(CACHE_TIME_OUT).addArgument(property).addArgument(CACHE_TIME_OUT_DEFAULT).log("could not parse {}='{}' return default timeout {} [minutes]");
            return CACHE_TIME_OUT_DEFAULT;
        }
    }

    public static TimeUnit getCacheTimeOutUnit() {
        return TimeUnit.MINUTES;
    }

    private static void printDiffs(final String title, final String unit, final List<Double> diffArray) {
        final double[] values = GenericsHelper.toDoublePrimitive(diffArray.toArray(new Double[0]));
        if (diffArray.size() >= 250) {
            final double mean = TMath.Mean(values);
            final double rms = TMath.RMS(values);
            final String msg = String.format("processing delays: %-15s  (%3d): dT = %4.1f +- %4.1f %s", title, diffArray.size(), mean, rms, unit);
            LOGGER.atInfo().log(msg);
            if (LOGGER.isDebugEnabled() && mean > 40.0) {
                LOGGER.atDebug().log(msg);
            }
            diffArray.clear();
        }
    }
}
