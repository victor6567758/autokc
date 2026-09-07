package com.zv.metrics;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PrometheusClient {

    public record MetricSample(Map<String, String> labels, double value) {}

    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl;

    public PrometheusClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public List<MetricSample> query(String promql) throws IOException, InterruptedException {
        String url = baseUrl + "/api/v1/query?query=" + URLEncoder.encode(promql, StandardCharsets.UTF_8);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Prometheus query failed: HTTP " + response.statusCode() + " - " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        if (!"success".equals(root.path("status").asText())) {
            throw new IOException("Prometheus query failed: " + root.path("errorType").asText("unknown error type")
                    + " - " + root.path("error").asText("no error message"));
        }

        List<MetricSample> samples = new ArrayList<>();
        for (JsonNode result : root.path("data").path("result")) {
            Map<String, String> labels = new LinkedHashMap<>();
            result.path("metric").fields().forEachRemaining(e -> labels.put(e.getKey(), e.getValue().asText()));
            JsonNode value = result.path("value");
            double v = value.isArray() && value.size() >= 2
                    ? Double.parseDouble(value.get(1).asText())
                    : Double.NaN;
            samples.add(new MetricSample(labels, v));
        }
        return samples;
    }
}
