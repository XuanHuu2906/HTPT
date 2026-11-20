package vn.nhom6.railway.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV6-D15: default harness tests require no Docker. Explicit opt-in gates:
 * cluster.smoke=true runs the real D14 lifecycle; cluster.g4.leader=true runs
 * strict SPEC-03 Leader checks after D15 integration. Neither certifies full G4.
 * Uses only the existing isolated compose.election-test.yml, never dev volumes.
 * TV6-D18 adds fake TCP booking evidence using the unchanged D17 runner.
 * Default CSV is target/AC03-results.csv; set -Dac03.csv=<absolute-path> to export.
 * Rows are explicitly FAKE_TCP, not Raft commit/SQL evidence. Real booking remains G5.
 */
class ClusterSmokeTest {
    private static final Set<String> NODE_IDS = Set.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Set<String> STATUS_FIELDS = Set.of("nodeId", "role", "term", "leaderId",
            "lastLogIndex", "commitIndex", "lastApplied", "storageHealthy", "peerConnectivity", "observedAt");

    @FunctionalInterface
    interface Action { void run() throws Exception; }

    /** Collect before stopping, even after partial startup or verification failure. */
    static void lifecycle(Action start, Action verify, Action collect, Action stop) throws Throwable {
        Throwable failure = null;
        try {
            start.run();
            verify.run();
        } catch (Throwable exception) {
            failure = exception;
        }
        for (Action cleanup : List.of(collect, stop)) {
            try {
                cleanup.run();
            } catch (Throwable exception) {
                if (failure == null) { failure = exception; }
                else { failure.addSuppressed(exception); }
            }
        }
        if (failure != null) { throw failure; }
    }

    @Test
    void harnessCollectsLogsBeforeStop() throws Throwable {
        List<String> actions = new ArrayList<>();
        lifecycle(() -> actions.add("start"), () -> actions.add("verifyLeader"),
                () -> actions.add("collect"), () -> actions.add("stop"));
        assertEquals(List.of("start", "verifyLeader", "collect", "stop"), actions);
    }

    @Test
    void harnessStillCollectsAndStopsAfterPartialStartupFailure() {
        List<String> actions = new ArrayList<>();
        IOException failure = new IOException("partial startup failed");
        Throwable actual = assertThrows(IOException.class, () -> lifecycle(
                () -> { actions.add("start"); throw failure; }, () -> actions.add("verifyLeader"),
                () -> actions.add("collect"), () -> actions.add("stop")));
        assertSame(failure, actual);
        assertEquals(List.of("start", "collect", "stop"), actions);
    }

    @Test
    void harnessPreservesVerificationFailureWhenCollectorAlsoFails() {
        List<String> actions = new ArrayList<>();
        IOException failure = new IOException("no stable Leader");
        Throwable actual = assertThrows(IOException.class, () -> lifecycle(
                () -> actions.add("start"), () -> { throw failure; },
                () -> { actions.add("collect"); throw new IOException("collector failed"); },
                () -> actions.add("stop")));
        assertSame(failure, actual);
        assertEquals(1, actual.getSuppressed().length);
        assertEquals(List.of("start", "collect", "stop"), actions);
    }

    @Test
    void verifyLeaderIsDynamicAndRequiresAllFiveHealthyNodesInOneTerm() {
        List<JsonNode> snapshot = fixture("node-2", false);
        assertEquals(new Leader("node-2", 8), identifyLeader(snapshot, false));
        ((ObjectNode) snapshot.get(0)).put("role", "LEADER");
        assertThrows(IllegalStateException.class, () -> identifyLeader(snapshot, false));
        assertThrows(IllegalStateException.class, () -> identifyLeader(snapshot.subList(0, 4), false));
        List<JsonNode> mixedTerms = fixture("node-4", false);
        ((ObjectNode) mixedTerms.get(0)).put("currentTerm", 9);
        assertThrows(IllegalStateException.class, () -> identifyLeader(mixedTerms, false));
    }

    @Test
    void legacyD14PayloadNeverPassesStrictG4LeaderGate() {
        assertThrows(IllegalStateException.class, () -> identifyLeader(fixture("node-3", false), true));
        assertEquals(new Leader("node-5", 8), identifyLeader(fixture("node-5", true), true));
        List<JsonNode> malformed = fixture("node-5", true);
        ((ObjectNode) malformed.get(0)).put("term", "8");
        assertThrows(IllegalStateException.class, () -> identifyLeader(malformed, true));
        List<JsonNode> lag = fixture("node-5", true);
        ((ObjectNode) lag.get(0)).put("lastApplied", 99);
        assertThrows(IllegalStateException.class, () -> identifyLeader(lag, true));
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void ac03SixBookingRunnerFixtureExportsExplicitFakeEvidence() throws Exception {
        runBookingFixture(false);
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void ac03LostResponsesRemainUnknownInFixtureEvidence() throws Exception {
        runBookingFixture(true);
    }

    private static void runBookingFixture(boolean dropResponses) throws Exception {
        String scenario = dropResponses ? "RESPONSE_DROP" : "NORMAL";
        System.out.println("TV6-D18 mode=FAKE_TCP scenario=" + scenario + "; real Raft and five DBs=NOT_RUN_G5");
        List<ConcurrentBookingRunner.Result> results;
        try (FakeTcpBookingServer server = new FakeTcpBookingServer(dropResponses)) {
            results = ConcurrentBookingRunner.run(InetAddress.getLoopbackAddress().getHostAddress(),
                    server.port(), "node-3", 3000);
            server.awaitHandled();
            assertEquals(6, results.size());
            assertEquals(6, results.stream().map(ConcurrentBookingRunner.Result::requestId).distinct().count());
            assertEquals(6, results.stream().map(ConcurrentBookingRunner.Result::messageId).distinct().count());
            assertEquals(server.requestIds, new HashSet<>(results.stream()
                    .map(ConcurrentBookingRunner.Result::requestId).toList()));
            assertEquals(server.messageIds, new HashSet<>(results.stream()
                    .map(ConcurrentBookingRunner.Result::messageId).toList()));
            assertTrue(results.stream().allMatch(result -> result.latencyNanos() >= 0));
            if (dropResponses) {
                assertTrue(results.stream().allMatch(result -> result.outcome().equals("UNKNOWN")
                        && result.reasonCode().equals("TIMEOUT_UNKNOWN") && result.appliedLogIndex() == null));
            } else {
                assertEquals(1, results.stream().filter(result -> result.outcome().equals("CONFIRMED")).count());
                assertEquals(5, results.stream().filter(result -> result.outcome().equals("REJECTED")
                        && "SEAT_ALREADY_BOOKED".equals(result.reasonCode())).count());
                assertTrue(results.stream().allMatch(result -> result.appliedLogIndex() != null && result.appliedLogIndex() > 0));
            }
        }
        // Export PASS only after response assertions AND socket/thread cleanup succeeded.
        exportBookingFixture(scenario, results);
    }

    private static synchronized void exportBookingFixture(String scenario,
            List<ConcurrentBookingRunner.Result> results) throws IOException {
        Path csv = Path.of(System.getProperty("ac03.csv", "target/AC03-results.csv"));
        if (csv.toAbsolutePath().getParent() != null) { Files.createDirectories(csv.toAbsolutePath().getParent()); }
        String header = "testId,runId,observedAtUtc,sourceRevision,testSourceSha256,runnerSourceSha256,sourceState,javaVersion,mode,fixture,scenario,client,tripId,seatId,requestId,messageId,latencyMs,outcome,reasonCode,fixtureAppliedLogIndex,detail,fixtureAssertion,realRaftG5,fiveDbConclusion,bookingRaftBridge\n";
        if (Files.exists(csv) && Files.size(csv) > 0) {
            try (var reader = Files.newBufferedReader(csv)) {
                if (!header.stripTrailing().equals(reader.readLine())) {
                    throw new IOException("Refusing to append fixture rows to an incompatible evidence CSV: " + csv);
                }
            }
        } else { Files.writeString(csv, header, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING); }
        String runId = UUID.randomUUID().toString();
        String at = Instant.now().toString();
        StringBuilder data = new StringBuilder();
        for (ConcurrentBookingRunner.Result result : results) {
            List<Object> values = java.util.Arrays.asList(
                    "TV6-D18-AC03-FIXTURE", runId, at,
                    System.getProperty("ac03.sourceRevision", "UNSPECIFIED"),
                    System.getProperty("ac03.testSourceSha256", "UNSPECIFIED"),
                    System.getProperty("ac03.runnerSourceSha256", "UNSPECIFIED"),
                    "WORKTREE_WITH_D18_TEST", System.getProperty("java.version"),
                    "FAKE_TCP", "FakeTcpBookingServer", scenario, result.client(), "SE-001", "G05",
                    result.requestId(), result.messageId(),
                    String.format(Locale.ROOT, "%.3f", result.latencyNanos() / 1_000_000.0),
                    result.outcome(), result.reasonCode(), result.appliedLogIndex(), result.detail(),
                    "PASS", "NOT_RUN_G5", "NOT_RUN_G5", "NOT_EXERCISED_FIXTURE_ONLY");
            data.append(values.stream().map(ClusterSmokeTest::csvCell)
                    .collect(java.util.stream.Collectors.joining(","))).append('\n');
        }
        Files.writeString(csv, data, StandardOpenOption.APPEND);
        System.out.println("TV6-D18 FIXTURE PASS scenario=" + scenario + " rows=6 runId=" + runId + "; CSV=" + csv.toAbsolutePath());
    }

    private static String csvCell(Object value) {
        return "\"" + (value == null ? "" : value.toString().replace("\"", "\"\"")) + "\"";
    }

    /** Loopback fixture only: one in-memory winner is not BookingRaftBridge or Raft. */
    private static final class FakeTcpBookingServer implements AutoCloseable {
        final boolean dropResponses;
        final ServerSocket listener;
        final ExecutorService acceptor = Executors.newSingleThreadExecutor();
        final ExecutorService handlers = Executors.newFixedThreadPool(6);
        final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        final Set<String> requestIds = ConcurrentHashMap.newKeySet();
        final Set<String> messageIds = ConcurrentHashMap.newKeySet();
        final CountDownLatch received = new CountDownLatch(6);
        final AtomicBoolean winner = new AtomicBoolean();
        final AtomicLong indexes = new AtomicLong();
        final List<Future<?>> tasks = new CopyOnWriteArrayList<>();
        final Future<?> accepting;

        FakeTcpBookingServer(boolean dropResponses) throws IOException {
            this.dropResponses = dropResponses;
            listener = new ServerSocket(0, 6, InetAddress.getLoopbackAddress());
            listener.setSoTimeout(5000);
            accepting = acceptor.submit(() -> {
                for (int n = 0; n < 6; n++) {
                    Socket socket = listener.accept();
                    socket.setSoTimeout(3000);
                    sockets.add(socket);
                    tasks.add(handlers.submit(() -> { handle(socket); return null; }));
                }
                return null;
            });
        }

        int port() { return listener.getLocalPort(); }

        void handle(Socket socket) throws Exception {
            try (socket) {
                MessageEnvelope<BookTicketRequestDto> request = FrameCodec.readMessage(socket.getInputStream(), BookTicketRequestDto.class);
                BookTicketRequestDto book = request.getPayload();
                book.validate();
                assertEquals(MessageType.BOOK_TICKET, request.getType());
                assertNull(request.getCorrelationId());
                assertEquals("SE-001", book.getTripId());
                assertEquals("G05", book.getSeatId());
                assertTrue(requestIds.add(book.getRequestId()), "Duplicate business intention");
                assertTrue(messageIds.add(request.getMessageId()), "Duplicate message ID");
                received.countDown();
                assertTrue(received.await(3, TimeUnit.SECONDS), "Did not receive six concurrent BOOK requests");
                if (dropResponses) { return; }
                long index = indexes.incrementAndGet();
                BookTicketResponseDto response = winner.compareAndSet(false, true)
                        ? BookTicketResponseDto.confirmed(book.getRequestId(), "SE-001", "G05", "fixture-D18-G05", index)
                        : BookTicketResponseDto.rejected(book.getRequestId(), "SE-001", "G05", "SEAT_ALREADY_BOOKED", index);
                response.validate();
                FrameCodec.writeMessage(socket.getOutputStream(), MessageEnvelope.createResponse(
                        request.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-3", response));
            } finally { sockets.remove(socket); }
        }

        void awaitHandled() throws Exception {
            accepting.get(5, TimeUnit.SECONDS);
            assertEquals(6, tasks.size());
            for (Future<?> task : tasks) { task.get(5, TimeUnit.SECONDS); }
        }

        @Override public void close() throws Exception {
            listener.close();
            for (Socket socket : sockets) {
                try { socket.close(); } catch (IOException ignored) { }
            }
            acceptor.shutdownNow();
            handlers.shutdownNow();
            assertTrue(acceptor.awaitTermination(3, TimeUnit.SECONDS), "Fixture acceptor leaked");
            assertTrue(handlers.awaitTermination(3, TimeUnit.SECONDS), "Fixture workers leaked");
        }
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @EnabledIfSystemProperty(named = "cluster.smoke", matches = "true")
    void realD14LifecycleAndLeaderGate() throws Throwable {
        runDockerGate(false);
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @EnabledIfSystemProperty(named = "cluster.g4.leader", matches = "true")
    void strictG4LeaderGateAfterD15Integration() throws Throwable {
        runDockerGate(true);
    }

    private static void runDockerGate(boolean strict) throws Throwable {
        DockerHarness harness = new DockerHarness(strict);
        String gate = strict ? "G4-LEADER" : "SMOKE-D14";
        harness.log("GATE=" + gate + "; full G4 booking/commit/apply/idempotency acceptance=NOT_RUN");
        harness.command("version", List.of("docker", "version", "--format", "{{.Server.Version}}"));
        harness.compose("config", "config", "--quiet");
        lifecycle(
                () -> harness.compose("start", "up", "-d", "--no-build", "--wait", "--wait-timeout", "70"),
                () -> {
                    harness.compose("ps-running", "ps", "-a");
                    harness.compose("images", "images");
                    Leader leader = harness.awaitLeader();
                    harness.log("REAL-LEADER PASS node=" + leader.nodeId() + " term=" + leader.term()
                            + " mode=" + (strict ? "SPEC-03-term" : "D14-currentTerm-compatibility"));
                }, harness::collectLogs, harness::stop);
        harness.log("GATE=" + gate + " PASS: start/verifyLeader/log collector/stop all completed");
        System.out.println("GATE=" + gate + " PASS; evidence=" + harness.evidence);
    }

    record Leader(String nodeId, long term) { }

    static Leader identifyLeader(List<JsonNode> snapshot, boolean strict) {
        check(snapshot.size() == 5, "Need five actual status responses");
        Set<String> ids = new HashSet<>();
        long commonTerm = -1;
        String leader = null;
        for (JsonNode status : snapshot) {
            String id = status.path("nodeId").asText();
            check(NODE_IDS.contains(id) && ids.add(id), "Unknown/duplicate node ID");
            check(status.path("storageHealthy").isBoolean() && status.get("storageHealthy").booleanValue(), "Storage unhealthy");
            String role = status.path("role").asText();
            check(Set.of("LEADER", "FOLLOWER", "CANDIDATE").contains(role), "Unknown/uninitialized role");
            long term = number(status, strict || status.has("term") ? "term" : "currentTerm");
            check(term > 0 && (commonTerm == -1 || term == commonTerm), "Terms not converged");
            commonTerm = term;
            long commit = number(status, "commitIndex");
            check(number(status, "lastApplied") <= commit, "Applied beyond commit");
            if (strict) {
                Set<String> fields = new HashSet<>();
                status.fieldNames().forEachRemaining(fields::add);
                check(fields.equals(STATUS_FIELDS), "SPEC-03 status field set mismatch");
                check(commit <= number(status, "lastLogIndex"), "Commit beyond log");
                JsonNode peers = status.path("peerConnectivity");
                Set<String> peerIds = new HashSet<>();
                peers.fieldNames().forEachRemaining(peerIds::add);
                Set<String> expectedPeers = new HashSet<>(NODE_IDS);
                expectedPeers.remove(id);
                check(peers.isObject() && peerIds.equals(expectedPeers), "SPEC-03 peer keys mismatch");
                peers.forEach(value -> check(value.isNull() || value.isBoolean(), "Invalid peer observation"));
                try { Instant.parse(status.path("observedAt").asText()); }
                catch (RuntimeException exception) { throw new IllegalStateException("Invalid observedAt", exception); }
            }
            if (role.equals("LEADER")) {
                check(leader == null, "Multiple reported Leaders");
                leader = id;
            }
        }
        check(leader != null, "No reported Leader");
        if (strict) {
            for (JsonNode status : snapshot) {
                check(leader.equals(status.path("leaderId").asText()), "Leader hints not converged");
            }
        }
        return new Leader(leader, commonTerm);
    }

    private static long number(JsonNode payload, String field) {
        JsonNode value = payload.path(field);
        check(value.isIntegralNumber() && value.canConvertToLong() && value.longValue() >= 0, "Invalid/missing " + field);
        return value.longValue();
    }

    private static void check(boolean condition, String message) {
        if (!condition) { throw new IllegalStateException(message); }
    }

    private static List<JsonNode> fixture(String leader, boolean strict) {
        List<JsonNode> result = new ArrayList<>();
        for (int n = 1; n <= 5; n++) {
            String id = "node-" + n;
            ObjectNode status = MessageCodec.getMapper().createObjectNode();
            status.put("nodeId", id).put("role", id.equals(leader) ? "LEADER" : "FOLLOWER")
                    .put(strict ? "term" : "currentTerm", 8L).put("commitIndex", 0L)
                    .put("lastApplied", 0L).put("storageHealthy", true);
            if (strict) {
                status.put("leaderId", leader).put("lastLogIndex", 0L).put("observedAt", "2026-10-29T10:00:00.000Z");
                ObjectNode peers = status.putObject("peerConnectivity");
                NODE_IDS.stream().filter(peer -> !peer.equals(id)).forEach(peer -> peers.put(peer, true));
            }
            result.add(status);
        }
        return result;
    }

    private static final class DockerHarness {
        final boolean strict;
        final Path root;
        final Path evidence;
        final String project = "railway-tv6-d15-" + UUID.randomUUID().toString().substring(0, 8);
        // Passed through environment only; never print Compose's resolved configuration.
        final String testPassword = UUID.randomUUID().toString();

        DockerHarness(boolean strict) throws IOException {
            this.strict = strict;
            Path candidate = Path.of("").toAbsolutePath();
            while (candidate != null && !Files.isRegularFile(candidate.resolve("docker/compose.election-test.yml"))) {
                candidate = candidate.getParent();
            }
            check(candidate != null, "Cannot locate repository root");
            root = candidate;
            evidence = Path.of(System.getProperty("cluster.evidence", root.resolve("integration-tests/target/" + project + ".log").toString()));
            if (evidence.getParent() != null) { Files.createDirectories(evidence.getParent()); }
            log("BEGIN " + Instant.now() + " project=" + project + " strict=" + strict);
        }

        void log(String line) throws IOException {
            Files.writeString(evidence, line + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        String command(String label, List<String> args) throws Exception {
            Path output = Files.createTempFile("railway-d15-command-", ".log");
            try {
                ProcessBuilder builder = new ProcessBuilder(args).directory(root.toFile())
                        .redirectErrorStream(true).redirectOutput(output.toFile());
                builder.environment().put("ELECTION_TEST_DB_PASSWORD", testPassword);
                log("COMMAND " + label + ": " + String.join(" ", args));
                Process process = builder.start();
                try {
                    if (!process.waitFor(95, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor(5, TimeUnit.SECONDS);
                        throw new IOException("Command deadline exceeded: " + label);
                    }
                    String text = Files.readString(output);
                    log("EXIT " + process.exitValue() + " " + label + System.lineSeparator() + text);
                    check(process.exitValue() == 0, "Command failed: " + label + "; see " + evidence);
                    return text;
                } finally {
                    if (process.isAlive()) { process.destroyForcibly(); }
                }
            } finally { Files.deleteIfExists(output); }
        }

        String compose(String label, String... args) throws Exception {
            List<String> command = new ArrayList<>(List.of("docker", "compose", "-p", project,
                    "-f", root.resolve("docker/compose.election-test.yml").toString()));
            command.addAll(List.of(args));
            return command(label, command);
        }

        Leader awaitLeader() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
            Leader previous = null;
            int stable = 0;
            String lastError = "no samples";
            while (System.nanoTime() < deadline) {
                try {
                    List<JsonNode> snapshot = new ArrayList<>();
                    for (int n = 1; n <= 5; n++) { snapshot.add(readStatus(n)); }
                    log("STATUS " + Instant.now() + " " + MessageCodec.getMapper().writeValueAsString(snapshot));
                    Leader current = identifyLeader(snapshot, strict);
                    stable = current.equals(previous) ? stable + 1 : 1;
                    previous = current;
                    if (stable >= 3) { return current; }
                } catch (IOException | IllegalStateException exception) {
                    stable = 0;
                    lastError = exception.getMessage();
                    log("WAIT " + lastError);
                }
                Thread.sleep(250);
            }
            throw new IOException("No three stable five-node Leader samples: " + lastError);
        }

        JsonNode readStatus(int number) throws IOException {
            MessageEnvelope<?> request = MessageEnvelope.createRequest(MessageType.NODE_STATUS,
                    "monitor-tv6-d15-smoke", MessageCodec.getMapper().createObjectNode());
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", 19100 + number), 1000);
                socket.setSoTimeout(1000);
                FrameCodec.writeMessage(socket.getOutputStream(), request);
                MessageEnvelope<JsonNode> response = FrameCodec.readMessage(socket.getInputStream());
                assertEquals(request.getMessageId(), response.getCorrelationId(), "Correlation mismatch");
                assertEquals("node-" + number, response.getSenderId(), "Wrong responding node");
                assertEquals(MessageType.NODE_STATUS_RESPONSE, response.getType(), "Unexpected response type");
                assertEquals("node-" + number, response.getPayload().path("nodeId").asText());
                return response.getPayload();
            }
        }

        void collectLogs() throws Exception {
            Exception failure = null;
            for (int n = 1; n <= 5; n++) {
                try {
                    String raw = compose("collect-node-" + n, "logs", "--no-color", "--no-log-prefix",
                            "--timestamps", "--tail", "200", "node-" + n);
                    check(!raw.isBlank(), "Empty runtime log for node-" + n);
                } catch (Exception exception) {
                    if (failure == null) { failure = exception; }
                    else { failure.addSuppressed(exception); }
                }
            }
            if (failure != null) { throw failure; }
            log("COLLECTOR PASS: five nonempty actual Docker backend logs captured before stop");
        }

        void stop() throws Exception {
            compose("stop", "stop", "--timeout", "5");
            check(compose("ps-after-stop", "ps", "-q").isBlank(), "Containers still running after stop");
            log("STOP PASS: own project stopped; no down -v, no database volume deletion");
        }
    }
}
