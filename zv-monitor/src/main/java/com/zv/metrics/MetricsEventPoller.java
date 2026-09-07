package com.zv.metrics;

import com.zv.event.Event;
import com.zv.event.EventBus;
import com.zv.event.EventSeverity;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MetricsEventPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsEventPoller.class);

    private final PrometheusClient prometheusClient;
    private final List<MetricPattern> patterns;
    private final EventBus eventBus;

    private final Map<String, Set<String>> firingSeriesByPattern = new ConcurrentHashMap<>();


    public void start(ScheduledExecutorService scheduler, long pollIntervalMs) {
        scheduler.scheduleAtFixedRate(this::pollOnce, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    void pollOnce() {
        for (MetricPattern pattern : patterns) {
            try {
                List<PrometheusClient.MetricSample> samples = prometheusClient.query(pattern.promql());
                Set<String> firing = samples.stream().map(MetricsEventPoller::seriesKey).collect(Collectors.toSet());
                Set<String> previous = firingSeriesByPattern.put(pattern.id(), firing);
                if (previous == null) {
                    previous = Set.of();
                }

                for (PrometheusClient.MetricSample sample : samples) {
                    String key = seriesKey(sample);
                    if (!previous.contains(key)) {
                        eventBus.publish(Event.metric(pattern.id(), pattern.severity(), pattern.container(),
                                pattern.description() + " | " + key + " value=" + sample.value()));
                    }
                }
                for (String key : previous) {
                    if (!firing.contains(key)) {
                        eventBus.publish(Event.metric(pattern.id() + "-recovered", EventSeverity.INFO,
                                pattern.container(), "condition cleared | " + key));
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Prometheus query failed for pattern '{}': {}", pattern.id(), e.getMessage());
            }
        }
    }

    private static String seriesKey(PrometheusClient.MetricSample sample) {
        return sample.labels().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
