package com.zv.logs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin client around Loki's query_range HTTP API.
 * Docs: https://grafana.com/docs/loki/latest/reference/loki-http-api/#query-loki-over-a-range-of-time
 */
public class LokiClient {

    /** One matched log line, already resolved to a concrete stream + timestamp. */
    public record LogHit(String container, long epochNanos, String line) {}

    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl;

    public LokiClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    /** startNanos/endNanos are epoch nanoseconds, per Loki's convention. */
    public List<LogHit> queryRange(String logQl, long startNanos, long endNanos, int limit)
            throws IOException, InterruptedException {
        String query = URLEncoder.encode(logQl, StandardCharsets.UTF_8);
        String url = baseUrl + "/loki/api/v1/query_range?query=" + query
                + "&start=" + startNanos + "&end=" + endNanos
                + "&limit=" + limit + "&direction=forward";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Loki query failed: HTTP " + response.statusCode() + " - " + response.body());
        }

        List<LogHit> hits = new ArrayList<>();
        JsonNode root = mapper.readTree(response.body());
        for (JsonNode stream : root.path("data").path("result")) {
            String container = stream.path("stream").path("service")
                    .asText(stream.path("stream").path("container").asText("unknown"));
            for (JsonNode value : stream.path("values")) {
                long ts = Long.parseLong(value.get(0).asText());
                String line = value.get(1).asText();
                hits.add(new LogHit(container, ts, line));
            }
        }
        return hits;
    }
}
