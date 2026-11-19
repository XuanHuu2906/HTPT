package vn.nhom6.railway.booking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcRequestResultStore;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kiểm thử cho {@link GetRequestResultHandler} theo SPEC-03 §4.4, SPEC-04 §7, SPEC-05.
 *
 * <p>Tách làm 2 nhóm:
 * <ul>
 *   <li><b>Nhóm không DB (In-Memory):</b> Dùng FakeReadPolicy và FakeResultStore, luôn chạy.</li>
 *   <li><b>Nhóm có DB (PostgreSQL):</b> Chỉ chạy khi có biến môi trường RAILWAY_TEST_*, tạo schema ngẫu nhiên
 *       {@code railway_test_*}, dọn dẹp sạch bằng {@code DROP SCHEMA ... CASCADE} trong finally.</li>
 * </ul>
 */
class GetRequestResultHandlerTest {

    private static final String REQUEST_ID_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_ID_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQUEST_ID_3 = "33333333-3333-3333-3333-333333333333";
    private static final String TICKET_ID_1 = "ticket-uuid-001";
    private static final String GOLDEN_FP_1 = "d2ce4e7bc0b8f00bd68dd016f15dfd5e1d8e0999493d22a298ec18c6e160c83a";
    private static final String GOLDEN_FP_2 = "9b841213b45fcb426eec780e5c7aa935d6d9141a53a9c9f80844e57aaf1b1375";

    /**
     * FakeReadPolicy - Lớp giả CHỈ DÙNG TRONG TEST, không phải RaftReadBarrier thật
     * (RaftReadBarrier thật của TV4-D19 là BLOCKED_INTEGRATION).
     */
    static final class FakeReadPolicy implements GetRequestResultHandler.ReadPolicy {
        private boolean leader = true;
        private String leaderHint = null;
        private boolean barrierSuccess = true;
        private String barrierErrorMessage = "A current read barrier is unavailable";

        public FakeReadPolicy asLeader() {
            this.leader = true;
            this.barrierSuccess = true;
            this.leaderHint = null;
            return this;
        }

        public FakeReadPolicy asFollower(String leaderHint) {
            this.leader = false;
            this.leaderHint = leaderHint;
            return this;
        }

        public FakeReadPolicy withBarrierFailure(String message) {
            this.leader = true;
            this.barrierSuccess = false;
            this.barrierErrorMessage = message;
            return this;
        }

        @Override
        public void ensureAuthoritative()
                throws BookingRequestHandler.NotLeaderException, BookingRequestHandler.ReadUnavailableException {
            if (!leader) {
                throw new BookingRequestHandler.NotLeaderException(leaderHint);
            }
            if (!barrierSuccess) {
                throw new BookingRequestHandler.ReadUnavailableException(barrierErrorMessage);
            }
        }

        @Override
        public Optional<String> leaderHint() {
            return Optional.ofNullable(leaderHint);
        }
    }

    /**
     * In-memory implementation of ResultStore for non-DB tests.
     */
    static final class InMemoryResultStore implements GetRequestResultHandler.ResultStore {
        private final Map<String, JdbcRequestResultStore.StoredRequestResult> store = new ConcurrentHashMap<>();

        public void put(JdbcRequestResultStore.StoredRequestResult result) {
            store.put(result.requestId(), result);
        }

        @Override
        public Optional<JdbcRequestResultStore.StoredRequestResult> find(String requestId) {
            return Optional.ofNullable(store.get(requestId));
        }
    }

    // =========================================================================
    // NHÓM 1: KIỂM THỬ KHÔNG CƠ SỞ DỮ LIỆU (LUÔN CHẠY)
    // =========================================================================

    @Nested
    @DisplayName("Nhóm 1: Kiểm thử logic In-Memory (không cần PostgreSQL)")
    class InMemoryTests {

        @Test
        @DisplayName("1. APPLIED_SUCCESS: Policy cho phép + có hàng CONFIRMED -> trả đúng ticketId và các trường SPEC")
        void test1_appliedSuccess_inMemory_returnsExactSpecFields() throws Exception {
            FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
            InMemoryResultStore store = new InMemoryResultStore();

            JdbcRequestResultStore.StoredRequestResult stored = new JdbcRequestResultStore.StoredRequestResult(
                    REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, "APPLIED_SUCCESS", null, TICKET_ID_1, 43L,
                    "{\"requestId\":\"" + REQUEST_ID_1 + "\",\"outcome\":\"CONFIRMED\",\"ticketId\":\"" + TICKET_ID_1 + "\"}");
            store.put(stored);

            GetRequestResultHandler handler = new GetRequestResultHandler(readPolicy, store, null);

            GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);

            assertEquals(REQUEST_ID_1, payload.requestId());
            assertEquals("APPLIED_SUCCESS", payload.state());
            assertEquals("BOOK_TICKET", payload.commandType());
            assertEquals(TICKET_ID_1, payload.ticketId());
            assertNull(payload.reasonCode());
            assertEquals(43L, payload.appliedLogIndex());
        }

        @Test
        @DisplayName("2. APPLIED_REJECTED: Có hàng REJECTED -> trả đúng reasonCode, ticketId=null")
        void test2_appliedRejected_inMemory_returnsExactSpecFields() throws Exception {
            FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
            InMemoryResultStore store = new InMemoryResultStore();

            JdbcRequestResultStore.StoredRequestResult stored = new JdbcRequestResultStore.StoredRequestResult(
                    REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, "APPLIED_REJECTED", "SEAT_ALREADY_BOOKED", null, 44L,
                    "{\"requestId\":\"" + REQUEST_ID_1 + "\",\"outcome\":\"REJECTED\",\"reasonCode\":\"SEAT_ALREADY_BOOKED\"}");
            store.put(stored);

            GetRequestResultHandler handler = new GetRequestResultHandler(readPolicy, store, null);

            GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);

            assertEquals(REQUEST_ID_1, payload.requestId());
            assertEquals("APPLIED_REJECTED", payload.state());
            assertEquals("BOOK_TICKET", payload.commandType());
            assertNull(payload.ticketId());
            assertEquals("SEAT_ALREADY_BOOKED", payload.reasonCode());
            assertEquals(44L, payload.appliedLogIndex());
        }

        @Test
        @DisplayName("3. PENDING: Adapter giữ requestId chưa apply -> PENDING, không phải REJECTED/UNKNOWN")
        void test3_pendingRequest_inMemory_returnsPendingState() throws Exception {
            FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
            InMemoryResultStore store = new InMemoryResultStore();

            RaftApplyAdapter adapter = new RaftApplyAdapter(
                    (idx, cmd) -> Optional.empty(),
                    null,
                    () -> 0L,
                    null);
            adapter.registerPending(REQUEST_ID_1);

            GetRequestResultHandler handler = new GetRequestResultHandler(
                    readPolicy, store, adapter, reqId -> Optional.of("BOOK_TICKET"));

            GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);

            assertEquals(REQUEST_ID_1, payload.requestId());
            assertEquals("PENDING", payload.state());
            assertEquals("BOOK_TICKET", payload.commandType());
            assertNull(payload.ticketId());
            assertNull(payload.reasonCode());
            assertNull(payload.appliedLogIndex());
        }

        @Test
        @DisplayName("4. UNKNOWN: Không biết gì về requestId -> trả đúng UNKNOWN theo SPEC, không kết luận thất bại")
        void test4_unknownRequest_inMemory_returnsUnknownState() throws Exception {
            FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
            InMemoryResultStore store = new InMemoryResultStore();

            GetRequestResultHandler handler = new GetRequestResultHandler(readPolicy, store, null);

            GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);

            assertEquals(REQUEST_ID_1, payload.requestId());
            assertEquals("UNKNOWN", payload.state());
            assertNull(payload.commandType());
            assertNull(payload.ticketId());
            assertNull(payload.reasonCode());
            assertNull(payload.appliedLogIndex());
        }

        @Test
        @DisplayName("5. Policy KHÔNG cho phép: Trả NOT_LEADER+leaderHint (kể cả hint null) hoặc READ_UNAVAILABLE")
        void test5_policyDisallows_followerAndBarrier_failClosed() {
            InMemoryResultStore store = new InMemoryResultStore();
            // Lưu sẵn hàng trong store để kiểm tra follower stale vẫn không được tự trả
            JdbcRequestResultStore.StoredRequestResult stored = new JdbcRequestResultStore.StoredRequestResult(
                    REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, "APPLIED_SUCCESS", null, TICKET_ID_1, 45L, "{}");
            store.put(stored);

            // 5a. Follower có hint "node-3"
            FakeReadPolicy policyFollowerWithHint = new FakeReadPolicy().asFollower("node-3");
            GetRequestResultHandler handler1 = new GetRequestResultHandler(policyFollowerWithHint, store, null);

            BookingRequestHandler.NotLeaderException ex1 = assertThrows(
                    BookingRequestHandler.NotLeaderException.class, () -> handler1.lookup(REQUEST_ID_1));
            assertEquals("node-3", ex1.leaderHint());

            GetRequestResultHandler.LookupOutcome outcome1 = handler1.handle(REQUEST_ID_1);
            assertInstanceOf(GetRequestResultHandler.LookupOutcome.NotLeader.class, outcome1);
            assertEquals("node-3", ((GetRequestResultHandler.LookupOutcome.NotLeader) outcome1).leaderHint());

            // 5b. Follower có hint null
            FakeReadPolicy policyFollowerNullHint = new FakeReadPolicy().asFollower(null);
            GetRequestResultHandler handler2 = new GetRequestResultHandler(policyFollowerNullHint, store, null);

            BookingRequestHandler.NotLeaderException ex2 = assertThrows(
                    BookingRequestHandler.NotLeaderException.class, () -> handler2.lookup(REQUEST_ID_1));
            assertNull(ex2.leaderHint());

            // 5c. Barrier failure
            FakeReadPolicy policyBarrierFailure = new FakeReadPolicy().withBarrierFailure("Read barrier timeout");
            GetRequestResultHandler handler3 = new GetRequestResultHandler(policyBarrierFailure, store, null);

            BookingRequestHandler.ReadUnavailableException ex3 = assertThrows(
                    BookingRequestHandler.ReadUnavailableException.class, () -> handler3.lookup(REQUEST_ID_1));
            assertTrue(ex3.getMessage().contains("Read barrier timeout"));

            GetRequestResultHandler.LookupOutcome outcome3 = handler3.handle(REQUEST_ID_1);
            assertInstanceOf(GetRequestResultHandler.LookupOutcome.ReadUnavailable.class, outcome3);
        }

        @Test
        @DisplayName("9. REQUEST_ID_CONFLICT: Cùng requestId nhưng khác fingerprint -> ném Conflict")
        void test9_requestIdConflict_inMemory_throwsConflict() throws Exception {
            FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
            InMemoryResultStore store = new InMemoryResultStore();

            JdbcRequestResultStore.StoredRequestResult stored = new JdbcRequestResultStore.StoredRequestResult(
                    REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, "APPLIED_SUCCESS", null, TICKET_ID_1, 43L, "{}");
            store.put(stored);

            GetRequestResultHandler handler = new GetRequestResultHandler(readPolicy, store, null);

            // Cùng fingerprint -> trả kết quả gốc
            GetRequestResultHandler.ResultPayload okPayload = handler.lookup(REQUEST_ID_1, GOLDEN_FP_1);
            assertEquals("APPLIED_SUCCESS", okPayload.state());

            // Không gửi fingerprint (lookup chuẩn SPEC-03 §4.4) -> trả kết quả gốc
            GetRequestResultHandler.ResultPayload pureLookup = handler.lookup(REQUEST_ID_1);
            assertEquals("APPLIED_SUCCESS", pureLookup.state());

            // Khác fingerprint -> ném RequestIdConflictException
            assertThrows(BookingRequestHandler.RequestIdConflictException.class,
                    () -> handler.lookup(REQUEST_ID_1, GOLDEN_FP_2));

            GetRequestResultHandler.LookupOutcome conflictOutcome = handler.handle(REQUEST_ID_1, GOLDEN_FP_2);
            assertInstanceOf(GetRequestResultHandler.LookupOutcome.Conflict.class, conflictOutcome);
        }

        @Test
        @DisplayName("Envelope handling: Đóng gói đúng message envelope và error envelope theo SPEC-03")
        void testEnvelopeHandling_matchesSpec() throws Exception {
            FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
            InMemoryResultStore store = new InMemoryResultStore();
            store.put(new JdbcRequestResultStore.StoredRequestResult(
                    REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, "APPLIED_SUCCESS", null, TICKET_ID_1, 50L, "{}"));

            GetRequestResultHandler handler = new GetRequestResultHandler(readPolicy, store, null);
            ObjectMapper mapper = new ObjectMapper();

            // Success envelope
            JsonNode reqJson = mapper.readTree("{\"requestId\":\"" + REQUEST_ID_1 + "\"}");
            MessageEnvelope<JsonNode> reqEnvelope = MessageEnvelope.createRequest(
                    MessageType.GET_REQUEST_RESULT, "client-1", reqJson);

            MessageEnvelope<?> resp = handler.handleEnvelope(reqEnvelope, "node-1");
            assertNotNull(resp);
            assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, resp.getType());
            assertEquals(reqEnvelope.getMessageId(), resp.getCorrelationId());

            // Invalid UUID
            JsonNode badJson = mapper.readTree("{\"requestId\":\"not-a-uuid\"}");
            MessageEnvelope<JsonNode> badReq = MessageEnvelope.createRequest(
                    MessageType.GET_REQUEST_RESULT, "client-1", badJson);
            MessageEnvelope<?> badResp = handler.handleEnvelope(badReq, "node-1");
            assertEquals(MessageType.ERROR_RESPONSE, badResp.getType());
            assertTrue(badResp.getPayload().toString().contains("INVALID_REQUEST"));
        }
    }

    // =========================================================================
    // NHÓM 2: KIỂM THỬ VỚI CƠ SỞ DỮ LIỆU POSTGRESQL THẬT
    // =========================================================================

    @Nested
    @DisplayName("Nhóm 2: Kiểm thử với PostgreSQL thật (Cô lập schema ngẫu nhiên)")
    class DatabaseTests {

        @Test
        @DisplayName("1. APPLIED_SUCCESS với DB thật: Ghi CONFIRMED -> đọc ra chính xác các trường SPEC-03")
        void test1_appliedSuccess_realDatabase() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, db.resultStore(store), null);

                // Chèn 1 kết quả thành công vào PostgreSQL
                try (Connection conn = db.connection()) {
                    ApplyResult result = ApplyResult.bookConfirmed(REQUEST_ID_1, TICKET_ID_1, 10L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);

                assertEquals(REQUEST_ID_1, payload.requestId());
                assertEquals("APPLIED_SUCCESS", payload.state());
                assertEquals("BOOK_TICKET", payload.commandType());
                assertEquals(TICKET_ID_1, payload.ticketId());
                assertNull(payload.reasonCode());
                assertEquals(10L, payload.appliedLogIndex());
            }
        }

        @Test
        @DisplayName("2. APPLIED_REJECTED với DB thật: Ghi REJECTED -> đọc ra đúng reasonCode, ticketId null")
        void test2_appliedRejected_realDatabase() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, db.resultStore(store), null);

                // Chèn 1 kết quả từ chối vào PostgreSQL
                try (Connection conn = db.connection()) {
                    ApplyResult result = ApplyResult.bookRejected(REQUEST_ID_1, "SEAT_ALREADY_BOOKED", 11L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);

                assertEquals(REQUEST_ID_1, payload.requestId());
                assertEquals("APPLIED_REJECTED", payload.state());
                assertEquals("BOOK_TICKET", payload.commandType());
                assertNull(payload.ticketId());
                assertEquals("SEAT_ALREADY_BOOKED", payload.reasonCode());
                assertEquals(11L, payload.appliedLogIndex());
            }
        }

        @Test
        @DisplayName("5. Follower stale có hàng trong DB nhưng policy không cho phép -> vẫn fail closed NOT_LEADER")
        void test5_followerStaleWithDataInDb_failsClosed() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                // Policy cấu hình là Follower
                FakeReadPolicy readPolicy = new FakeReadPolicy().asFollower("node-2");
                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, db.resultStore(store), null);

                // Dù PostgreSQL đã có hàng
                try (Connection conn = db.connection()) {
                    ApplyResult result = ApplyResult.bookConfirmed(REQUEST_ID_1, TICKET_ID_1, 12L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                // Follower tuyệt đối không tự trả kết quả quyền uy
                BookingRequestHandler.NotLeaderException ex = assertThrows(
                        BookingRequestHandler.NotLeaderException.class, () -> handler.lookup(REQUEST_ID_1));
                assertEquals("node-2", ex.leaderHint());

                GetRequestResultHandler.LookupOutcome outcome = handler.handle(REQUEST_ID_1);
                assertInstanceOf(GetRequestResultHandler.LookupOutcome.NotLeader.class, outcome);
                assertEquals("node-2", ((GetRequestResultHandler.LookupOutcome.NotLeader) outcome).leaderHint());
            }
        }

        @Test
        @DisplayName("6. Follower stale không trả AVAILABLE: Không suy kết quả từ bảng seats")
        void test6_staleFollowerNeverReturnsAvailable_neverInfersFromSeats() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, db.resultStore(store), null);

                // Trường hợp 6a: Bảng seats đang là AVAILABLE, nhưng request_results có CONFIRMED
                try (Connection conn = db.connection()) {
                    // Đảm bảo ghế G05 trong bảng seats là AVAILABLE
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("UPDATE seats SET status = 'AVAILABLE', current_ticket_id = NULL WHERE seat_id = 'G05'");
                    }
                    ApplyResult result = ApplyResult.bookConfirmed(REQUEST_ID_1, TICKET_ID_1, 15L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                GetRequestResultHandler.ResultPayload payloadA = handler.lookup(REQUEST_ID_1);
                // Phải trả APPLIED_SUCCESS theo request_results, tuyệt đối KHÔNG trả hay ngụ ý AVAILABLE
                assertEquals("APPLIED_SUCCESS", payloadA.state());
                assertEquals(TICKET_ID_1, payloadA.ticketId());
                assertNotEquals("AVAILABLE", payloadA.state());

                // Trường hợp 6b: Bảng seats đang là BOOKED, nhưng request_results KHÔNG có hàng cho REQUEST_ID_2
                try (Connection conn = db.connection()) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("INSERT INTO tickets(ticket_id, trip_id, seat_id, passenger_name, status, issued_at) "
                                + "VALUES ('ticket-other', 'SE-001', 'G05', 'Khach Khac', 'ACTIVE', NOW())");
                        stmt.execute("UPDATE seats SET status = 'BOOKED', current_ticket_id = 'ticket-other' WHERE seat_id = 'G05'");
                    }
                }

                GetRequestResultHandler.ResultPayload payloadB = handler.lookup(REQUEST_ID_2);
                // Phải trả UNKNOWN, tuyệt đối không suy từ seats mà tự bịa CONFIRMED hay REJECTED
                assertEquals("UNKNOWN", payloadB.state());
                assertNull(payloadB.ticketId());
                assertNull(payloadB.commandType());

                // Trường hợp 6c: Khi policy là Follower, không trả AVAILABLE mà trả NOT_LEADER
                readPolicy.asFollower("node-3");
                BookingRequestHandler.NotLeaderException ex = assertThrows(
                        BookingRequestHandler.NotLeaderException.class, () -> handler.lookup(REQUEST_ID_1));
                assertEquals("node-3", ex.leaderHint());
            }
        }

        @Test
        @DisplayName("7. Kết quả đã lưu không đổi qua các lần lookup lặp lại; handler không ghi gì vào DB")
        void test7_repeatLookupsAreIdempotent_noDatabaseWritesOccur() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, db.resultStore(store), null);

                // Chèn 1 kết quả
                try (Connection conn = db.connection()) {
                    ApplyResult result = ApplyResult.bookConfirmed(REQUEST_ID_1, TICKET_ID_1, 20L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                // Đếm số lượng hàng trước khi gọi lookup
                long resultsCountBefore = db.countRows("request_results");
                long seatsCountBefore = db.countRows("seats");
                long ticketsCountBefore = db.countRows("tickets");
                long eventsCountBefore = db.countRows("business_events");

                // Thực hiện 5 lần lookup liên tiếp
                for (int i = 0; i < 5; i++) {
                    GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);
                    assertEquals(REQUEST_ID_1, payload.requestId());
                    assertEquals("APPLIED_SUCCESS", payload.state());
                    assertEquals(TICKET_ID_1, payload.ticketId());
                    assertEquals(20L, payload.appliedLogIndex());
                }

                // Thực hiện lookup cho requestId không tồn tại
                GetRequestResultHandler.ResultPayload unknownPayload = handler.lookup(REQUEST_ID_2);
                assertEquals("UNKNOWN", unknownPayload.state());

                // Đếm lại số lượng hàng sau khi lookup: phải không đổi (0 ghi)
                assertEquals(resultsCountBefore, db.countRows("request_results"));
                assertEquals(seatsCountBefore, db.countRows("seats"));
                assertEquals(ticketsCountBefore, db.countRows("tickets"));
                assertEquals(eventsCountBefore, db.countRows("business_events"));
            }
        }

        @Test
        @DisplayName("8. Lỗi DB (chèn lỗi bằng Proxy): fail closed, không trả APPLIED_*")
        void test8_databaseError_usingProxy_failsClosed() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();

                // Tạo ResultStore bọc Connection bằng Proxy để giả lập lỗi truy vấn SQL
                GetRequestResultHandler.ResultStore failingStore = reqId -> {
                    try (Connection realConn = db.connectionUnchecked();
                         Connection proxyConn = (Connection) Proxy.newProxyInstance(
                                 Connection.class.getClassLoader(),
                                 new Class<?>[]{Connection.class},
                                 (proxy, method, args) -> {
                                     if ("prepareStatement".equals(method.getName())
                                             || "createStatement".equals(method.getName())) {
                                         throw new SQLException("Simulated PostgreSQL disk failure", "08006", 1001);
                                     }
                                     return method.invoke(realConn, args);
                                 })) {
                        return store.find(proxyConn, reqId);
                    } catch (SQLException e) {
                        throw new BookingRepository.StorageException("Simulated PostgreSQL disk failure", e);
                    }
                };

                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, failingStore, null);

                // lookup phải fail closed, ném StorageException
                BookingRepository.StorageException ex = assertThrows(
                        BookingRepository.StorageException.class, () -> handler.lookup(REQUEST_ID_1));
                assertTrue(ex.getMessage().contains("Simulated PostgreSQL disk failure")
                        || (ex.getCause() != null && ex.getCause().getMessage().contains("Simulated PostgreSQL disk failure")));

                // handle phải trả về StorageError, tuyệt đối không trả APPLIED_*
                GetRequestResultHandler.LookupOutcome outcome = handler.handle(REQUEST_ID_1);
                assertInstanceOf(GetRequestResultHandler.LookupOutcome.StorageError.class, outcome);
            }
        }

        @Test
        @DisplayName("9. REQUEST_ID_CONFLICT trên DB thật: ném RequestIdConflictException khi fingerprint khác")
        void test9_requestIdConflict_realDatabase() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();
                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, db.resultStore(store), null);

                try (Connection conn = db.connection()) {
                    ApplyResult result = ApplyResult.bookConfirmed(REQUEST_ID_1, TICKET_ID_1, 30L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                // Cùng fingerprint -> OK
                GetRequestResultHandler.ResultPayload ok = handler.lookup(REQUEST_ID_1, GOLDEN_FP_1);
                assertEquals("APPLIED_SUCCESS", ok.state());

                // Khác fingerprint -> RequestIdConflictException
                assertThrows(BookingRequestHandler.RequestIdConflictException.class,
                        () -> handler.lookup(REQUEST_ID_1, GOLDEN_FP_2));
            }
        }

        @Test
        @DisplayName("fromJdbc constructor test: Kiểm tra hàm tiện ích fromJdbc với ConnectionFactory thật")
        void testFromJdbc_withRealConnectionFactory() throws Exception {
            try (TestDatabase db = TestDatabase.create()) {
                JdbcRequestResultStore store = new JdbcRequestResultStore();
                FakeReadPolicy readPolicy = new FakeReadPolicy().asLeader();

                URI uri = URI.create(db.jdbcUrl.substring("jdbc:".length()));
                String databaseName = uri.getPath().substring(1);
                int port = uri.getPort() < 0 ? 5432 : uri.getPort();
                String dbNameWithSchema = databaseName + "?currentSchema=" + db.schema;
                DbConfig config = new DbConfig(uri.getHost(), port, dbNameWithSchema, db.username, db.password);
                ConnectionFactory schemaFactory = new ConnectionFactory(config);

                GetRequestResultHandler handler = new GetRequestResultHandler(
                        readPolicy, schemaFactory, store, null);

                // Chèn 1 kết quả
                try (Connection conn = db.connection()) {
                    ApplyResult result = ApplyResult.bookConfirmed(REQUEST_ID_1, TICKET_ID_1, 40L);
                    store.insert(conn, REQUEST_ID_1, "BOOK_TICKET", GOLDEN_FP_1, result, "SE-001", "G05");
                }

                GetRequestResultHandler.ResultPayload payload = handler.lookup(REQUEST_ID_1);
                assertEquals("APPLIED_SUCCESS", payload.state());
                assertEquals(TICKET_ID_1, payload.ticketId());
            }
        }
    }

    // =========================================================================
    // HELPER: CƠ SỞ DỮ LIỆU KIỂM THỬ CÔ LẬP
    // =========================================================================

    static final class TestDatabase implements AutoCloseable {
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final DbConfig config;
        private final ConnectionFactory baseFactory;

        TestDatabase(String jdbcUrl, String username, String password, String schema, DbConfig config, ConnectionFactory baseFactory) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = schema;
            this.config = config;
            this.baseFactory = baseFactory;
        }

        static TestDatabase create() throws Exception {
            String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
            String username = System.getenv("RAILWAY_TEST_DB_USER");
            String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
            Assumptions.assumeTrue(
                    jdbcUrl != null && username != null && password != null,
                    "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            String databaseName = uri.getPath().substring(1);
            int port = uri.getPort() < 0 ? 5432 : uri.getPort();
            DbConfig config = new DbConfig(uri.getHost(), port, databaseName, username, password);
            ConnectionFactory factory = new ConnectionFactory(config);
            String schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");

            try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA \"" + schema + "\"");
            }

            TestDatabase database = new TestDatabase(jdbcUrl, username, password, schema, config, factory);
            try {
                database.runMigration(readMigration("V1__schema.sql"));
                database.runMigration(readMigration("V2__seed.sql"));
                return database;
            } catch (Exception exception) {
                try {
                    database.close();
                } catch (SQLException cleanupFailure) {
                    exception.addSuppressed(cleanupFailure);
                }
                throw exception;
            }
        }

        public GetRequestResultHandler.ResultStore resultStore(JdbcRequestResultStore store) {
            return requestId -> {
                try (Connection conn = connection()) {
                    return store.find(conn, requestId);
                } catch (SQLException e) {
                    throw new BookingRepository.StorageException("Failed to query request_results", e);
                }
            };
        }

        public Connection connection() throws SQLException {
            Connection connection = baseFactory.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET search_path TO \"" + schema + "\"");
                return connection;
            } catch (SQLException exception) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
                throw exception;
            }
        }

        public Connection connectionUnchecked() {
            try {
                return connection();
            } catch (SQLException exception) {
                throw new IllegalStateException("Could not open test schema connection", exception);
            }
        }

        public long countRows(String tableName) throws SQLException {
            try (Connection conn = connection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + tableName + "\"")) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }

        private void runMigration(String migration) throws SQLException {
            try (Connection connection = connection();
                 Statement statement = connection.createStatement()) {
                for (String sql : migration.split(";")) {
                    if (!sql.isBlank()) {
                        statement.execute(sql);
                    }
                }
            }
        }

        private static String readMigration(String name) throws IOException {
            Path local = Path.of("database", "migration", name);
            Path parent = Path.of("..", "database", "migration", name);
            if (Files.isRegularFile(local)) {
                return Files.readString(local, StandardCharsets.UTF_8);
            }
            if (Files.isRegularFile(parent)) {
                return Files.readString(parent, StandardCharsets.UTF_8);
            }
            throw new IOException("Migration not found: " + local.toAbsolutePath());
        }

        @Override
        public void close() throws SQLException {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
        }
    }
}
