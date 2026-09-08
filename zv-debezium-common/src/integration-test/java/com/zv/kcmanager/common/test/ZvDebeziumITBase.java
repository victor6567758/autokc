package com.zv.kcmanager.common.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import com.zv.kcmanager.common.util.FixedPortPostgresContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ZvDebeziumITBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZvDebeziumITBase.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(6);

    // Docker network aliases - mirrors the compose service names.
    public static final String KAFKA_ALIAS = "kafka";
    public static final String KAFKA_CONNECT_ALIAS = "kafka-connect";
    public static final String POSTGRES_ALIAS = "postgres";
    public static final int KAFKA_INTERNAL_PORT = 29092;   // PLAINTEXT, in-network
    public static final int KAFKA_HOST_PORT = 9092;        // PLAINTEXT_HOST, bound to a free host port
    public static final int KAFKA_CONNECT_PORT = 8083;     // Connect REST API
    public static final String SCHEMA_REGISTRY_ALIAS = "schema-registry";   // mirrors the compose service name
    public static final int SCHEMA_REGISTRY_PORT = 8080;   // Apicurio Registry REST API
    public static final String SCHEMA_REGISTRY_CCOMPAT = "/apis/ccompat/v7";   // Confluent-compatible REST base path
    public static final int KAFKA_CONNECT_DEBUG_PORT = 5005;

    // The single Postgres shared by all tests of a class.
    public static final String DB_NAME = "zvdb";
    public static final String DB_USER = "postgres";
    public static final String DB_PASSWORD = "postgres";

    protected final Network network = Network.newNetwork();
    protected GenericContainer<?> kafka;
    protected GenericContainer<?> schemaRegistry;
    protected PostgreSQLContainer<?> postgres;
    protected GenericContainer<?> kafkaConnect;

    private final HttpClient http = HttpClient.newHttpClient();
    private final List<ContainerLogWriter> logWriters = new ArrayList<>();
    private KafkaProducer<String, String> producer;
    private int kafkaHostPort;
    private int postgresHostPort;

    /** The newest {@code target/zv-debezium-connector-*.tar.gz} of the module under test. */
    protected static Optional<Path> findPluginTarball() {
        Path targetDir = Paths.get("target");
        if (!Files.isDirectory(targetDir)) {
            return Optional.empty();
        }
        try (Stream<Path> files = Files.list(targetDir)) {
            return files
                    .filter(path -> path.getFileName().toString().matches("zv-debezium-connector-.*\\.tar\\.gz"))
                    .max(Comparator.comparing(path -> path.getFileName().toString()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @BeforeAll
    void startStack() throws IOException {
        kafkaHostPort = findFreeHostPort();
        postgresHostPort = findFreeHostPort();

        // Kafka broker
        kafka = new GenericContainer<>(DockerImageName.parse("apache/kafka:3.7.0"))
                .withNetwork(network)
                .withNetworkAliases(KAFKA_ALIAS)
                .withCreateContainerCmdModifier(this::bindKafkaHostPort)
                .withEnv("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + KAFKA_ALIAS + ":29093")
                .withEnv("KAFKA_LISTENERS",
                        "PLAINTEXT://0.0.0.0:" + KAFKA_INTERNAL_PORT + ",CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:" + KAFKA_HOST_PORT)
                .withEnv("KAFKA_ADVERTISED_LISTENERS",
                        "PLAINTEXT://" + KAFKA_ALIAS + ":" + KAFKA_INTERNAL_PORT + ",PLAINTEXT_HOST://localhost:" + kafkaHostPort)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withLogConsumer(logConsumer(KAFKA_ALIAS))
                .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));

        // Apicurio Registry 3.x (Apache-2.0, CNCF sandbox) in kafkasql mode -
        // backed by the stack's Kafka broker, no extra database. Mirrors the
        // compose stack's schema-registry service. The Confluent-compatible
        // REST API lives under SCHEMA_REGISTRY_CCOMPAT. The worker keeps
        // JsonConverter as its default but pre-binds
        // key/value.converter.schema.registry.url to schemaRegistryNetworkUrl(),
        // so a connector-level converter override (Avro/Protobuf/JSON-Schema)
        // talks to the registry out of the box; test-side consumers use
        // schemaRegistryUrl() (host-mapped).
        schemaRegistry = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry:3.3.1"))
                .withNetwork(network)
                .withNetworkAliases(SCHEMA_REGISTRY_ALIAS)
                .withEnv("APICURIO_STORAGE_KIND", "kafkasql")
                .withEnv("APICURIO_KAFKASQL_BOOTSTRAP_SERVERS", KAFKA_ALIAS + ":" + KAFKA_INTERNAL_PORT)
                .withExposedPorts(SCHEMA_REGISTRY_PORT)
                .dependsOn(kafka)
                .withLogConsumer(logConsumer(SCHEMA_REGISTRY_ALIAS))
                .waitingFor(Wait.forHttp(SCHEMA_REGISTRY_CCOMPAT + "/subjects")
                        .forPort(SCHEMA_REGISTRY_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(STARTUP_TIMEOUT));

        // Postgres - the stack's single database (CDC origin or sink target).
        postgres = new FixedPortPostgresContainer(DockerImageName.parse("postgres:16"), postgresHostPort)
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_ALIAS)
                .withDatabaseName(DB_NAME)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
                .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10")
                .withLogConsumer(logConsumer(POSTGRES_ALIAS));

        List<GenericContainer<?>> containers = new ArrayList<>(List.of(kafka, schemaRegistry, postgres));

        // Kafka Connect worker
        Path pluginTarball = findPluginTarball().orElseThrow(() -> new IllegalStateException("No plugin tarball (target/zv-debezium-connector-*.tar.gz) found "));
        LOGGER.info("Building Kafka Connect worker image from plugin tarball {}", pluginTarball);
        kafkaConnect = connectWorkerContainer(pluginTarball);
        containers.add(kafkaConnect);

        Startables.deepStart(containers).join();
        waitForKafkaReady();
        LOGGER.info("Stack ready; Kafka Connect REST at {}, Schema Registry at {}", connectRestUrl(), schemaRegistryUrl());
    }

    @AfterAll
    void stopStack() {
        if (producer != null) {
            producer.close();
        }
        Stream.of(kafkaConnect, schemaRegistry, postgres, kafka)
                .filter(Objects::nonNull)
                .forEach(GenericContainer::stop);
        network.close();
        logWriters.forEach(ContainerLogWriter::close);
        logWriters.clear();
    }

    // ------------------------------------------------------------------ stack

    private void bindKafkaHostPort(CreateContainerCmd cmd) {
        cmd.getHostConfig().withPortBindings(
                new PortBinding(Ports.Binding.bindPort(kafkaHostPort), new ExposedPort(KAFKA_HOST_PORT)));
    }

    private GenericContainer<?> connectWorkerContainer(Path pluginTarball) throws IOException {
        ImageFromDockerfile image = new ImageFromDockerfile("zv-it-kafka-connect", true)
                .withFileFromPath("plugin.tar.gz", pluginTarball)
                .withFileFromString("connect-distributed.properties", connectWorkerProperties())
                .withDockerfileFromBuilder(builder -> builder
                        .from("apache/kafka:3.7.0")
                        // ADD extracts the tarball; its root dir (artifactId) lands in /opt/connect-plugins/.
                        .add("plugin.tar.gz", "/opt/connect-plugins/")
                        .add("connect-distributed.properties", "/opt/kafka/config/connect-distributed.properties")
                        .env("KAFKA_HEAP_OPTS", "-Xms256m -Xmx1g")
                        .expose(KAFKA_CONNECT_PORT)
                        .build());
        FixedPortContainer worker = new FixedPortContainer(image)
                .withNetwork(network)
                .withNetworkAliases(KAFKA_CONNECT_ALIAS)
                .withExposedPorts(KAFKA_CONNECT_PORT)
                // the base image's ENTRYPOINT starts a broker - run the Connect worker instead
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withEntrypoint("/opt/kafka/bin/connect-distributed.sh")
                        .withCmd("/opt/kafka/config/connect-distributed.properties"))
                .dependsOn(kafka, schemaRegistry, postgres)
                .withLogConsumer(logConsumer(KAFKA_CONNECT_ALIAS))
                .waitingFor(Wait.forHttp("/connectors")
                        .forPort(KAFKA_CONNECT_PORT)
                        .withStartupTimeout(STARTUP_TIMEOUT));
        if (connectDebugEnabled()) {
            enableConnectDebugging(worker);
        }
        return worker;
    }

    /**
     * Opens the worker JVM for remote debugging: JDWP listens inside the container and its port is
     * published to a fixed host port, so an IDE remote-debugger config (localhost:5005) can attach
     * to the connector code running there (streamkap {@code KafkaFacade}'s {@code debugKCExpose}).
     */
    private void enableConnectDebugging(FixedPortContainer worker) {
        boolean suspend = connectDebugSuspend();
        // connect-distributed.sh -> kafka-run-class.sh appends KAFKA_JVM_PERFORMANCE_OPTS to the
        // worker JVM command line, so the jdwp agent rides along (heap stays on KAFKA_HEAP_OPTS).
        worker.withEnv("KAFKA_JVM_PERFORMANCE_OPTS",
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=" + (suspend ? "y" : "n")
                        + ",address=*:" + KAFKA_CONNECT_DEBUG_PORT);
        // addFixedExposedPort (not a CreateContainerCmd port-binding override) so Testcontainers
        // keeps its own random host binding for the exposed REST port and merely adds this one.
        worker.publishFixedPort(KAFKA_CONNECT_DEBUG_PORT, KAFKA_CONNECT_DEBUG_PORT);
        LOGGER.info("Connect worker remote debugging: attach a JDWP debugger to localhost:{} (suspend={})",
                KAFKA_CONNECT_DEBUG_PORT, suspend);
    }

    /** Subclassing merely exposes Testcontainers' protected {@code addFixedExposedPort} (see {@link #enableConnectDebugging}). */
    private static final class FixedPortContainer extends GenericContainer<FixedPortContainer> {

        FixedPortContainer(ImageFromDockerfile image) {
            super(image);
        }

        void publishFixedPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
        }
    }

    private static boolean connectDebugEnabled() {
        return Boolean.parseBoolean(flag("zv.it.debug.connect", "IT_DEBUG_CONNECT"));
    }

    private static boolean connectDebugSuspend() {
        return Boolean.parseBoolean(flag("zv.it.debug.connect.suspend", "IT_DEBUG_CONNECT_SUSPEND"));
    }

    private static String flag(String systemProperty, String envVar) {
        return System.getProperty(systemProperty, System.getenv().getOrDefault(envVar, "false"));
    }

    /** Worker properties; override to adapt source vs sink worker setups. */
    protected String connectWorkerProperties() {
        return String.join("\n",
                "bootstrap.servers=" + KAFKA_ALIAS + ":" + KAFKA_INTERNAL_PORT,
                "group.id=zv-it-connect",
                "rest.port=" + KAFKA_CONNECT_PORT,
                "rest.advertised.host.name=" + KAFKA_CONNECT_ALIAS,
                "config.storage.topic=connect-configs",
                "offset.storage.topic=connect-offsets",
                "status.storage.topic=connect-status",
                "config.storage.replication.factor=1",
                "offset.storage.replication.factor=1",
                "status.storage.replication.factor=1",
                "offset.flush.interval.ms=1000",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable=true",
                "value.converter.schemas.enable=true",
                // Registry binding for schema-managed converters: the worker points
                // key/value.converter.schema.registry.url at the stack's registry
                // (in-network ccompat URL), so a connector-level converter override
                // (Avro/Protobuf/JSON-Schema) talks to it out of the box. The default
                // JSON converters ignore these keys - the wire format stays JSON.
                "key.converter.schema.registry.url=" + schemaRegistryNetworkUrl(),
                "value.converter.schema.registry.url=" + schemaRegistryNetworkUrl(),
                "internal.key.converter=org.apache.kafka.connect.json.JsonConverter",
                "internal.value.converter=org.apache.kafka.connect.json.JsonConverter",
                "internal.key.converter.schemas.enable=false",
                "internal.value.converter.schemas.enable=false",
                "plugin.path=/opt/connect-plugins",
                "");
    }

    private void waitForKafkaReady() {
        Awaitility.await("Kafka broker answers metadata requests")
                .atMost(STARTUP_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    try (Admin admin = Admin.create(clientProps())) {
                        admin.listTopics().names().get();
                        return true;
                    }
                    catch (Exception e) {
                        return false;
                    }
                });
    }

    private static int findFreeHostPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Logs every container to slf4j and to {@code target/container-logs/<alias>.log} for post-mortem digging. */
    private Consumer<OutputFrame> logConsumer(String alias) throws IOException {
        Slf4jLogConsumer slf4j = new Slf4jLogConsumer(LoggerFactory.getLogger("stack." + alias)).withPrefix(alias);
        ContainerLogWriter writer = new ContainerLogWriter(alias);
        logWriters.add(writer);
        return frame -> {
            slf4j.accept(frame);
            writer.accept(frame);
        };
    }

    private static final class ContainerLogWriter implements Consumer<OutputFrame> {

        private final BufferedWriter writer;

        ContainerLogWriter(String alias) throws IOException {
            Path dir = Paths.get("target", "container-logs");
            Files.createDirectories(dir);
            writer = Files.newBufferedWriter(dir.resolve(alias + ".log"), StandardCharsets.UTF_8);
        }

        @Override
        public void accept(OutputFrame frame) {
            if (frame == null || frame.getBytes() == null) {
                return;
            }
            try {
                writer.write(frame.getUtf8String());
                writer.flush();
            }
            catch (IOException e) {
                LOGGER.warn("Failed to write container log: {}", e.getMessage());
            }
        }

        void close() {
            try {
                writer.close();
            }
            catch (IOException e) {
                // ignored
            }
        }
    }

    // ------------------------------------------------------------ Kafka access

    /** Bootstrap address reachable from the test JVM (PLAINTEXT_HOST listener). */
    public String bootstrapServers() {
        return "localhost:" + kafkaHostPort;
    }

    /** Bootstrap address reachable from containers on the stack network (PLAINTEXT listener). */
    public String internalBootstrapServers() {
        return KAFKA_ALIAS + ":" + KAFKA_INTERNAL_PORT;
    }

    private Map<String, Object> clientProps() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers());
        return props;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = clientProps();
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "zv-it-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        return props;
    }

    public void createTopic(String topic) {
        try (Admin admin = Admin.create(clientProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to create topic " + topic, e);
        }
    }

    public void deleteTopic(String topic) {
        try (Admin admin = Admin.create(clientProps())) {
            admin.deleteTopics(List.of(topic)).all().get();
            LOGGER.info("Deleted topic '{}'", topic);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to delete topic '{}': {}", topic, e.getMessage());
        }
    }

    public void awaitTopic(String topic, Duration timeout) {
        Awaitility.await("topic " + topic + " to exist")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    try (Admin admin = Admin.create(clientProps())) {
                        return admin.listTopics().names().get().contains(topic);
                    }
                    catch (Exception e) {
                        return false;
                    }
                });
    }

    /** Sends one keyed record; a {@code null} value produces a tombstone. */
    public void send(String topic, String key, String value) {
        if (producer == null) {
            Map<String, Object> props = clientProps();
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            props.put("acks", "all");
            producer = new KafkaProducer<>(props);
        }
        try {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to send record to " + topic, e);
        }
    }

    /** Waits until the topic carries {@code minCount} matching records; each attempt drains the topic from the beginning. */
    public List<ConsumerRecord<String, String>> waitForRecords(String topic, int minCount, Duration timeout,
            Predicate<List<ConsumerRecord<String, String>>> predicate) {
        return Awaitility.await("at least " + minCount + " records on topic " + topic)
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> drainTopic(topic, minCount), records -> records.size() >= minCount && predicate.test(records));
    }

    public List<ConsumerRecord<String, String>> waitForRecords(String topic, int minCount, Duration timeout) {
        return waitForRecords(topic, minCount, timeout, records -> true);
    }

    private List<ConsumerRecord<String, String>> drainTopic(String topic, int minCount) {
        long deadline = System.currentTimeMillis() + 4_000;
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {

            List<TopicPartition> partitions = null;
            while (System.currentTimeMillis() < deadline) {
                var infos = consumer.partitionsFor(topic);
                if (infos != null && !infos.isEmpty()) {
                    partitions = infos.stream()
                            .map(info -> new TopicPartition(topic, info.partition()))
                            .toList();
                    break;
                }
                sleepQuietly(200);
            }
            if (partitions == null) {
                return records;   // topic not created yet
            }

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (System.currentTimeMillis() < deadline && records.size() < minCount) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(200));
                polled.forEach(records::add);
            }
        }
        return records;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------- Connect REST API

    /** Base URL of the Connect REST API, reachable from the test JVM. */
    public String connectRestUrl() {
        return "http://" + kafkaConnect.getHost() + ":" + kafkaConnect.getMappedPort(KAFKA_CONNECT_PORT);
    }

    /** Base URL of the registry's Confluent-compatible REST API, reachable from the test JVM. */
    public String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT) + SCHEMA_REGISTRY_CCOMPAT;
    }

    /** In-network Confluent-compatible registry URL for connector configs ({@code schema.registry.url}). */
    public String schemaRegistryNetworkUrl() {
        return "http://" + SCHEMA_REGISTRY_ALIAS + ":" + SCHEMA_REGISTRY_PORT + SCHEMA_REGISTRY_CCOMPAT;
    }

    /** Creates (or idempotently updates) a connector via {@code PUT /connectors/{name}/config}. */
    public void createConnector(String name, Map<String, String> config) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(connectRestUrl() + "/connectors/" + name + "/config"))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(config)))
                    .build();
            HttpResponse<String> response = http.send(request, BodyHandlers.ofString());
            if (response.statusCode() != 200 && response.statusCode() != 201) {
                throw new IllegalStateException("Failed to create connector '" + name + "' (HTTP " + response.statusCode()
                        + "): " + response.body());
            }
            LOGGER.info("Connector '{}' created", name);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating connector '" + name + "'", e);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to create connector '" + name + "'", e);
        }
    }

    /** Deletes the connector if it exists; task shutdown is awaited by Connect itself. */
    public void deleteConnector(String name) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(connectRestUrl() + "/connectors/" + name)).DELETE().build();
            HttpResponse<String> response = http.send(request, BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                return;
            }
            if (response.statusCode() != 204) {
                LOGGER.warn("Failed to delete connector '{}' (HTTP {}): {}", name, response.statusCode(), response.body());
                return;
            }
            LOGGER.info("Connector '{}' deleted", name);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to delete connector '{}': {}", name, e.getMessage());
        }
    }

    /** {@code GET /connectors/{name}/status}; an empty object when the connector does not exist (yet). */
    public JsonNode connectorStatus(String name) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(connectRestUrl() + "/connectors/" + name + "/status")).GET().build();
            HttpResponse<String> response = http.send(request, BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return JSON.createObjectNode();
            }
            return JSON.readTree(response.body());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return JSON.createObjectNode();
        }
        catch (Exception e) {
            return JSON.createObjectNode();
        }
    }

    /** Waits until both the connector and its first task are RUNNING (task trace surfaces on failure). */
    public void waitForConnectorRunning(String name, Duration timeout) {
        Awaitility.await("connector '" + name + "' to reach RUNNING")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    JsonNode status = connectorStatus(name);
                    assertThat(status.path("connector").path("state").asText())
                            .as("connector state (status: %s)", status)
                            .isEqualTo("RUNNING");
                    JsonNode tasks = status.path("tasks");
                    assertThat(tasks.isArray() && !tasks.isEmpty())
                            .as("connector tasks (status: %s)", status)
                            .isTrue();
                    JsonNode task = tasks.get(0);
                    assertThat(task.path("state").asText())
                            .as("task state (trace: %s)", task.path("trace").asText(""))
                            .isEqualTo("RUNNING");
                });
    }

    // -------------------------------------------------------------- SQL access

    /** Host-side JDBC connection to the stack's Postgres (from the test JVM). */
    public Connection openDbConnection() throws SQLException {
        return DriverManager.getConnection(dbHostUrl(), DB_USER, DB_PASSWORD);
    }

    public String dbHostUrl() {
        return "jdbc:postgresql://" + postgres.getHost() + ":" + postgresHostPort + "/" + DB_NAME;
    }

    /** JDBC URL for connector configs: in-network address of the database. */
    public String dbNetworkUrl() {
        return "jdbc:postgresql://" + POSTGRES_ALIAS + ":5432/" + DB_NAME;
    }
}
