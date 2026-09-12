package com.zv.logs;

import com.zv.event.Event;
import com.zv.event.EventBus;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Polls Loki on a fixed interval, one LogQL query per LogPattern, and turns
 * matched lines into Events on the shared EventBus.
 *
 * Loki is also queried directly by Grafana (Explore / log panels) for humans
 * to read raw logs - this poller is the only thing that needs to "understand"
 * which lines matter enough to become an Event.
 */
@RequiredArgsConstructor
public class LogEventPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogEventPoller.class);

    private final LokiClient lokiClient;
    private final List<LogPattern> patterns;
    private final EventBus eventBus;
    private final long lookbackNanosOnStartup;

    /**
     * Promtail/Loki ingestion-lag guard. A line logged at T often only becomes
     * queryable in Loki at T+shipping-lag; a poller that advances its watermark
     * straight to "now" will never see it (the tick before it landed excluded
     * it by not-yet-ingested, every tick after excludes it by timestamp).
     * So each tick re-queries OVERLAP behind the watermark and de-duplicates
     * replays by line timestamp - an exact re-see has the same nanosecond ts.
     */
    private static final long REQUERY_OVERLAP_NANOS = TimeUnit.SECONDS.toNanos(15);

    /** Per-pattern watermark: nanos already covered, so each tick only asks Loki for what's new. */
    private final Map<String, Long> lastEndNanosByPattern = new ConcurrentHashMap<>();

    /** Per-pattern newest timestamp actually published as an Event (replay dedup). */
    private final Map<String, Long> lastPublishedNanosByPattern = new ConcurrentHashMap<>();

    public void start(ScheduledExecutorService scheduler, long pollIntervalMs) {
        scheduler.scheduleAtFixedRate(this::pollOnce, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    void pollOnce() {
        long now = System.currentTimeMillis() * 1_000_000L;
        for (LogPattern pattern : patterns) {
            long start = lastEndNanosByPattern.computeIfAbsent(pattern.id(), k -> now - lookbackNanosOnStartup);
            try {
                long queryStart = Math.max(start - REQUERY_OVERLAP_NANOS, now - lookbackNanosOnStartup);
                List<LokiClient.LogHit> hits = lokiClient.queryRange(pattern.toLogQl(), queryStart, now, 100);
                long lastPublished = lastPublishedNanosByPattern.getOrDefault(pattern.id(), Long.MIN_VALUE);
                long maxTs = start;
                for (LokiClient.LogHit hit : hits) {
                    if (hit.epochNanos() > lastPublished) {
                        eventBus.publish(Event.log(pattern.id(), pattern.severity(), hit.container(), hit.line(),
                                Instant.ofEpochMilli(hit.epochNanos() / 1_000_000L)));
                        lastPublishedNanosByPattern.put(pattern.id(), hit.epochNanos());
                        lastPublished = hit.epochNanos();
                    }
                    maxTs = Math.max(maxTs, hit.epochNanos() + 1);
                }
                // never run the watermark further ahead than overlap behind now,
                // so late-shipped lines stay inside the next query window
                lastEndNanosByPattern.put(pattern.id(), Math.max(maxTs, now - REQUERY_OVERLAP_NANOS));
            } catch (Exception e) {
                LOGGER.warn("Loki query failed for pattern '{}': {}", pattern.id(), e.getMessage());
                // leave the watermark where it was so the same window is retried next tick
            }
        }
    }
}
