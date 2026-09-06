package com.zv.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin client around the Kafka Connect REST API.
 * Docs: https://kafka.apache.org/documentation/#connect_rest
 */
public class ConnectClient {

    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl;

    public ConnectClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /** GET /connectors/{name}/status */
    public ConnectorStatus getConnectorStatus(String connectorName) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/connectors/" + connectorName + "/status"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 404) {
            return ConnectorStatus.notFound(connectorName);
        }
        if (response.statusCode() != 200) {
            return ConnectorStatus.unreachable(connectorName,
                    "HTTP " + response.statusCode() + ": " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        String connectorState = root.path("connector").path("state").asText("UNKNOWN");
        String workerId = root.path("connector").path("worker_id").asText(null);
        String trace = root.path("connector").path("trace").asText(null);

        List<TaskStatus> tasks = new ArrayList<>();
        for (JsonNode taskNode : root.path("tasks")) {
            tasks.add(new TaskStatus(
                    taskNode.path("id").asInt(-1),
                    taskNode.path("state").asText("UNKNOWN"),
                    taskNode.path("worker_id").asText(null),
                    taskNode.path("trace").asText(null)
            ));
        }

        return ConnectorStatus.healthy(connectorName, connectorState, workerId, trace, tasks);
    }

    /** GET /connectors - list all registered connector names. Useful for auto-discovery. */
    public List<String> listConnectors() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/connectors"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode root = mapper.readTree(response.body());
        List<String> names = new ArrayList<>();
        root.forEach(n -> names.add(n.asText()));
        return names;
    }

    /** POST /connectors/{name}/restart - used by remediation handlers. */
    public int restartConnector(String connectorName, boolean includeTasks) throws IOException, InterruptedException {
        String query = includeTasks ? "?includeTasks=true" : "";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/connectors/" + connectorName + "/restart" + query))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }

    /** POST /connectors/{name}/tasks/{id}/restart */
    public int restartTask(String connectorName, int taskId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/connectors/" + connectorName + "/tasks/" + taskId + "/restart"))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }
}
