package vn.nhom6.railway.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.booking.*;
import vn.nhom6.railway.networking.*;
import vn.nhom6.railway.protocol.dto.*;
import vn.nhom6.railway.raft.core.RaftLogEntry;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Real TCP and PostgreSQL. Commit delivery is a controlled harness, NOT a Raft cluster. */
class JdbcBookingTcpIntegrationTest {
    @Test
    void tcpWaitsForDurableApplyAndPreservesReplayConflictAndSnapshot() throws Exception {
        try (Database db = new Database()) {
            var submitted = new LinkedBlockingQueue<Pending>();
            NodeConfig config = db.config();
            try (var node = NodeMain.withPersistentBooking(config, command -> {
                var result = new CompletableFuture<ApplyResult>();
                submitted.add(new Pending(command, result));
                return result;
            }, timeout -> CompletableFuture.completedFuture(
                    new BookingRequestHandler.ReadBarrierResult(db.repository.lastApplied(), "node-1")), null);
                 var executor = Executors.newSingleThreadExecutor()) {
                node.start();
                String requestId = "11111111-1111-1111-1111-111111111111";
                var first = executor.submit(() -> book(node, requestId, "G02", "Demo Passenger"));
                Pending pending = submitted.poll(5, TimeUnit.SECONDS);
                assertNotNull(pending);
                assertFalse(first.isDone(), "No response before controlled commit delivery");
                assertEquals(0, db.count("tickets"));
                db.apply(1, pending);
                JsonNode confirmed = first.get(5, TimeUnit.SECONDS).getPayload();
                assertEquals("CONFIRMED", confirmed.get("outcome").asText());
                assertEquals(1, db.count("tickets"));
                assertEquals(1, db.count("request_results"));
                assertEquals(1, db.repository.lastApplied());

                var retry = executor.submit(() -> book(node, requestId, "G02", "Demo Passenger"));
                db.apply(2, Objects.requireNonNull(submitted.poll(5, TimeUnit.SECONDS)));
                assertEquals(confirmed, retry.get(5, TimeUnit.SECONDS).getPayload());
                assertEquals(1, db.count("tickets"));
                var conflict = executor.submit(() -> book(node, requestId, "G03", "Demo Passenger"));
                db.apply(3, Objects.requireNonNull(submitted.poll(5, TimeUnit.SECONDS)));
                var conflictResponse = conflict.get(5, TimeUnit.SECONDS);
                assertEquals(MessageType.ERROR_RESPONSE, conflictResponse.getType());
                assertEquals("REQUEST_ID_CONFLICT", conflictResponse.getPayload().get("errorCode").asText());
                var occupied = executor.submit(() -> book(node, UUID.randomUUID().toString(), "G02", "Another Demo"));
                db.apply(4, Objects.requireNonNull(submitted.poll(5, TimeUnit.SECONDS)));
                assertEquals("SEAT_ALREADY_BOOKED", occupied.get(5, TimeUnit.SECONDS).getPayload().get("reasonCode").asText());
                assertEquals(1, db.count("tickets"));
                // Harness supplies an applied NO_OP as the read barrier; no consensus claim.
                db.storage.replaceSuffix(5, List.of(new RaftLogEntry(5, 1, "NO_OP", "{}")));
                db.applier.applyCommitted(5, new BookingCommand.NoOp());
                try (var client = new TcpClient("127.0.0.1", node.getClientPort(), 2000, 5000)) {
                    client.connect();
                    var response = client.sendAndReceive(MessageEnvelope.createRequest(
                            MessageType.GET_SEATS, "client-db-test", Map.of("tripId", "SE-001")));
                    assertEquals(MessageType.GET_SEATS_RESPONSE, response.getType());
                    JsonNode seats = response.getPayload().get("seats");
                    assertEquals(10, seats.size());
                    assertEquals("BOOKED", seats.get(1).get("status").asText());
                    assertEquals(confirmed.get("ticketId"), seats.get(1).get("ticketId"));
                }
                System.out.println("SQL EVIDENCE (isolated schema; controlled commit harness, NOT real Raft): " + db.snapshot());
            }
        }
    }

    @Test
    void defaultBackendWithDatabaseCannotConfirmWithoutRaft() throws Exception {
        try (Database db = new Database(); var node = new NodeMain(db.config())) {
            node.start();
            var response = book(node, UUID.randomUUID().toString(), "G02", "Demo");
            assertEquals(MessageType.ERROR_RESPONSE, response.getType());
            assertEquals("NO_QUORUM", response.getPayload().get("errorCode").asText());
            assertEquals(0, db.count("tickets"));
            assertEquals(0, db.repository.lastApplied());
        }
    }

    private static MessageEnvelope<JsonNode> book(NodeMain node, String id, String seat, String name) throws Exception {
        try (var client = new TcpClient("127.0.0.1", node.getClientPort(), 2000, 10000)) {
            client.connect();
            var request = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-db-test",
                    Map.of("requestId", id, "tripId", "SE-001", "seatId", seat, "passengerName", name));
            var response = client.sendAndReceive(request);
            assertEquals(request.getMessageId(), response.getCorrelationId());
            return response;
        }
    }

    private record Pending(BookingCommand.BookTicket command, CompletableFuture<ApplyResult> result) {}

    private static final class Database implements AutoCloseable {
        final String url = Objects.requireNonNull(System.getenv("RAILWAY_TEST_JDBC_URL"), "Real PostgreSQL required");
        final String user = Objects.requireNonNull(System.getenv("RAILWAY_TEST_DB_USER"));
        final String password = Objects.requireNonNull(System.getenv("RAILWAY_TEST_DB_PASSWORD"));
        final String schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
        final DbConfig dbConfig;
        final JdbcBookingRepository repository;
        final JdbcRaftStorage storage;
        final JdbcCommittedCommandApplier applier;
        Database() throws Exception {
            URI uri = URI.create(url.substring(5));
            dbConfig = new DbConfig(uri.getHost(), uri.getPort(),
                    uri.getPath().substring(1) + "?currentSchema=" + schema, user, password);
            try (Connection c = DriverManager.getConnection(url, user, password); Statement s = c.createStatement()) {
                s.execute("CREATE SCHEMA " + schema);
            }
            try {
                try (Connection c = connection(); Statement s = c.createStatement()) {
                    for (String migration : List.of("V1__schema.sql", "V2__seed.sql")) {
                        Path file = Path.of("database/migration", migration);
                        if (!Files.exists(file)) file = Path.of("..").resolve(file);
                        s.execute(Files.readString(file));
                    }
                }
                var factory = new ConnectionFactory(dbConfig);
                repository = new JdbcBookingRepository(factory);
                storage = new JdbcRaftStorage(factory);
                applier = new JdbcCommittedCommandApplier(factory);
                storage.saveTermAndVote(1, "node-1");
            } catch (Exception failure) { close(); throw failure; }
        }
        NodeConfig config() {
            return new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, dbConfig, 8, 10000);
        }
        Connection connection() throws SQLException {
            return DriverManager.getConnection(dbConfig.jdbcUrl(), user, password);
        }
        void apply(long index, Pending pending) throws Exception {
            var command = pending.command();
            storage.replaceSuffix(index, List.of(new RaftLogEntry(index, 1, "BOOK_TICKET",
                    new ObjectMapper().writeValueAsString(Map.of(
                            "requestId", command.requestId(), "tripId", command.tripId(),
                            "seatId", command.seatId(), "passengerName", command.passengerName(),
                            "ticketId", command.ticketId(), "issuedAt", command.issuedAt().toString())))));
            var outcome = applier.applyCommitted(index, command).orElseThrow();
            if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult business) {
                pending.result().complete(business.applyResult());
            } else {
                pending.result().completeExceptionally(new BookingRequestHandler.RequestIdConflictException());
            }
        }
        int count(String table) throws SQLException {
            try (var c = connection(); var s = c.createStatement(); var r = s.executeQuery("SELECT count(*) FROM " + table)) {
                r.next(); return r.getInt(1);
            }
        }
        String snapshot() throws SQLException {
            try (var c = connection(); var s = c.createStatement(); var r = s.executeQuery(
                    "SELECT json_build_object('seats',(SELECT json_agg(s ORDER BY seat_id) FROM seats s),"
                    + "'tickets',(SELECT json_agg(t) FROM tickets t),'results',(SELECT json_agg(r ORDER BY applied_log_index) FROM request_results r),"
                    + "'lastApplied',(SELECT last_applied FROM raft_apply_progress))::text")) {
                r.next(); return r.getString(1);
            }
        }
        @Override public void close() throws SQLException {
            try (var c = DriverManager.getConnection(url, user, password); var s = c.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }
}
