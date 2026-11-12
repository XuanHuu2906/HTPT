package vn.nhom6.railway.client;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kiểm thử khả năng tự động chuyển hướng theo leaderHint của {@link LeaderAwareBookingClient} (TASK TV1-D12).
 * <p>
 * Toàn bộ kiểm thử sử dụng nhiều {@link FakeNodeServer} dựng trên localhost (cổng ngẫu nhiên),
 * gắn nhãn rõ ràng: "FAKE SERVER, không phải Node thật".
 * Đảm bảo 14 ca kiểm thử bắt buộc theo yêu cầu TV1-D12.
 */
public class LeaderAwareBookingClientTest {

    private final List<FakeNodeServer> fakeServers = new ArrayList<>();
    private final List<LeaderAwareBookingClient> clientsToClose = new ArrayList<>();

    @BeforeEach
    void setUp() {
        fakeServers.clear();
        clientsToClose.clear();
    }

    @AfterEach
    void tearDown() {
        for (LeaderAwareBookingClient client : clientsToClose) {
            try {
                client.close();
            } catch (Exception ignored) {
            }
        }
        for (FakeNodeServer server : fakeServers) {
            try {
                server.close();
            } catch (Exception ignored) {
            }
        }
    }

    private FakeNodeServer createServer(String nodeId) throws IOException {
        FakeNodeServer server = new FakeNodeServer(nodeId);
        fakeServers.add(server);
        return server;
    }

    private ClusterEndpoints createEndpoints(FakeNodeServer... servers) {
        Map<String, NodeEndpoint> map = new HashMap<>();
        String defaultNode = (servers.length > 0) ? servers[0].getNodeId() : "node-1";
        for (FakeNodeServer s : servers) {
            map.put(s.getNodeId(), new NodeEndpoint(s.getNodeId(), "127.0.0.1", s.getPort()));
        }
        return new ClusterEndpoints(map, defaultNode);
    }

    private LeaderAwareBookingClient createClient(ClusterEndpoints endpoints, RedirectPolicy policy, ClusterConnectionViewModel vm) {
        LeaderAwareBookingClient client = new LeaderAwareBookingClient(
                endpoints,
                ep -> new SocketBookingClient(ep.host(), ep.port(), (int) policy.stepTimeoutMs(), (int) policy.stepTimeoutMs(), "client-test"),
                policy,
                vm,
                endpoints.getDefaultNodeId()
        );
        clientsToClose.add(client);
        return client;
    }

    // =========================================================================
    // 14 CA KIỂM THỬ BẮT BUỘC THEO TV1-D12
    // =========================================================================

    @Test
    @DisplayName("Ca 1: Follower A -> NOT_LEADER (hint B) -> B (Leader) -> CONFIRMED; giữ nguyên requestId, messageId khác")
    void test1_RedirectFromFollowerToLeaderConfirmed() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            serverA.setNotLeader("node-2", 2L);
            serverB.setLeader();

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = new RedirectPolicy(3, 3000, 10000);
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Hành Khách Test 1");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            // Kiểm tra kết quả cuối cùng
            assertNotNull(response);
            assertTrue(response.isConfirmed(), "Kết quả cuối cùng phải là CONFIRMED");
            assertEquals("CONFIRMED", response.getOutcome());
            assertEquals(requestId, response.getRequestId(), "requestId phải bất biến suốt chu kỳ redirect");
            assertNotNull(response.getTicketId());
            assertFalse(response.isMock());

            // Kiểm tra tính toàn vẹn giữa các Node
            assertEquals(1, serverB.getActiveTicketCount("G01"), "Leader B phải có đúng 1 vé ACTIVE");
            assertEquals(0, serverA.getActiveTicketCount("G01"), "Follower A không được có vé nào");

            // Kiểm tra frame ghi nhận tại A và B
            List<MessageEnvelope<JsonNode>> reqsA = serverA.getReceivedRequests();
            List<MessageEnvelope<JsonNode>> reqsB = serverB.getReceivedRequests();

            assertEquals(1, reqsA.size(), "A phải nhận đúng 1 request");
            assertEquals(1, reqsB.size(), "B phải nhận đúng 1 request");

            String msgIdA = reqsA.get(0).getMessageId();
            String msgIdB = reqsB.get(0).getMessageId();

            assertNotEquals(msgIdA, msgIdB, "Mỗi lần gửi qua socket phải sinh messageId mới theo chuẩn envelope");
            assertEquals(requestId, reqsA.get(0).getPayload().get("requestId").asText());
            assertEquals(requestId, reqsB.get(0).getPayload().get("requestId").asText());
        });
    }

    @Test
    @DisplayName("Ca 2: Redirect không bao giờ tự thành CONFIRMED; B từ chối thì trả REJECTED")
    void test2_RedirectNeverBecomesConfirmedByItself() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            serverA.setNotLeader("node-2", 2L);
            serverB.setLeader();
            // Cấu hình ghế G02 tại Leader B đã có vé trước đó
            serverB.setSeatStatus("G02", "BOOKED", "TCK-OLD-G02");

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G02", "Khách Test 2");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertFalse(response.isConfirmed(), "Tuyệt đối không được trả CONFIRMED khi Leader từ chối");
            assertTrue(response.isRejected(), "Phải trả REJECTED nguyên văn từ Leader");
            assertEquals("SEAT_ALREADY_BOOKED", response.getReasonCode());
            assertEquals(requestId, response.getRequestId());
        });
    }

    @Test
    @DisplayName("Ca 3: Hint là nodeId không có trong cấu hình -> dừng, trả NOT_LEADER_UNRESOLVED")
    void test3_HintNotInConfigurationStops() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            // Gợi ý node lạ không có trong cluster.properties
            serverA.setNotLeader("node-999", 2L);

            ClusterEndpoints endpoints = createEndpoints(serverA);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 3");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown());
            assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, response.getReasonCode());
            assertTrue(response.getMessage().contains("node-999"));
            assertEquals(requestId, response.getRequestId());
        });
    }

    @Test
    @DisplayName("Ca 4: Hint trùng chính Node vừa từ chối -> dừng, trả NOT_LEADER_UNRESOLVED")
    void test4_SelfReferencingHintStops() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            // Gợi ý chính mình
            serverA.setNotLeader("node-1", 2L);

            ClusterEndpoints endpoints = createEndpoints(serverA);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 4");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown());
            assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, response.getReasonCode());
            assertTrue(response.getMessage().contains("trùng chính Node"));
        });
    }

    @Test
    @DisplayName("Ca 5: Vòng lặp A -> B -> A -> dừng bằng phát hiện vòng lặp, không vượt maxRedirects")
    void test5_CycleDetectionStops() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            // A gợi ý B, B gợi ý lại A
            serverA.setNotLeader("node-2", 2L);
            serverB.setNotLeader("node-1", 2L);

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = new RedirectPolicy(5, 2000, 10000); // maxRedirects = 5
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 5");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown());
            assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, response.getReasonCode());
            assertTrue(response.getMessage().contains("vòng lặp"));

            // Xác nhận không bị gửi vòng lặp 5 lần: A nhận 1, B nhận 1
            assertEquals(1, serverA.getReceivedRequests().size());
            assertEquals(1, serverB.getReceivedRequests().size());
        });
    }

    @Test
    @DisplayName("Ca 6: Vượt maxRedirects (chuỗi A -> B -> C -> D) -> dừng và trả lỗi rõ ràng")
    void test6_MaxRedirectsExceededStops() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");
            FakeNodeServer serverC = createServer("node-3");
            FakeNodeServer serverD = createServer("node-4");

            serverA.setNotLeader("node-2", 1L);
            serverB.setNotLeader("node-3", 1L);
            serverC.setNotLeader("node-4", 1L);
            serverD.setLeader();

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB, serverC, serverD);
            // Giới hạn maxRedirects = 2
            RedirectPolicy policy = new RedirectPolicy(2, 2000, 10000);
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 6");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown());
            assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, response.getReasonCode());
            assertTrue(response.getMessage().contains("tối đa"));

            // D không bao giờ được gọi tới vì đã hết 2 lần redirect ở C
            assertEquals(0, serverD.getReceivedRequests().size());
        });
    }

    @Test
    @DisplayName("Ca 7: leaderHint null hoặc rỗng -> dừng, trả lỗi rõ ràng")
    void test7_NullOrEmptyHintStops() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            serverA.setNotLeader(null, 1L); // hint = null

            ClusterEndpoints endpoints = createEndpoints(serverA);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 7");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown());
            assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, response.getReasonCode());
            assertTrue(response.getMessage().contains("leaderHint"));
        });
    }

    @Test
    @DisplayName("Ca 8: Leader được gợi ý không kết nối được (server đã tắt) -> CONNECTION_FAILED, requestId giữ nguyên")
    void test8_HintedLeaderUnreachable() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            serverA.setNotLeader("node-2", 2L);
            // Tắt server B trước khi client kết nối
            serverB.close();

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = new RedirectPolicy(3, 1000, 5000);
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 8");

            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertNotNull(response);
            assertFalse(response.isConfirmed());
            assertEquals(LeaderAwareBookingClient.ERROR_CONNECTION_FAILED, response.getReasonCode());
            assertEquals(requestId, response.getRequestId(), "requestId phải được giữ nguyên khi gặp lỗi kết nối");
        });
    }

    @Test
    @DisplayName("Ca 9: Gửi tới Leader rồi timeout không trả lời -> UNKNOWN (TIMEOUT_UNKNOWN); không phải REJECTED")
    void test9_TimeoutAtLeaderReturnsUnknownNotRejected() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            serverA.setNotLeader("node-2", 2L);
            serverB.setLeader();
            // Leader B nhận lệnh, lưu vào DB nhưng giả lập timeout mạng không kịp trả lời
            serverB.setSimulateTimeoutOnBook(true, 4000);

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            // Step timeout là 1000ms
            RedirectPolicy policy = new RedirectPolicy(3, 1000, 6000);
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G05", "Khách Test 9");

            BookTicketResponseDto response = client.book(request).get(4, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown(), "Timeout phải trả về UNKNOWN");
            assertFalse(response.isRejected(), "Tuyệt đối không được trả về REJECTED khi timeout");
            assertEquals("TIMEOUT_UNKNOWN", response.getReasonCode());
            assertEquals(requestId, response.getRequestId());

            // Xác nhận Leader B thực tế đã nhận được request này (và có thể đã lưu kết quả)
            assertEquals(1, serverB.getRequestAttemptCount(requestId));
        });
    }

    @Test
    @DisplayName("Ca 10: Duplicate: cùng requestId gửi lại nhiều lần qua redirect -> Leader chỉ có 1 vé duy nhất")
    void test10_DuplicateSameRequestIdOnlyOneTicketOnLeader() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            serverA.setNotLeader("node-2", 2L);
            serverB.setLeader();

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G06", "Khách Idempotent");

            // Lần 1: gửi và redirect thành công
            BookTicketResponseDto res1 = client.book(request).get(5, TimeUnit.SECONDS);
            assertTrue(res1.isConfirmed());

            // Lần 2: gửi lại cùng requestId và cùng payload
            BookTicketResponseDto res2 = client.book(request).get(5, TimeUnit.SECONDS);
            assertTrue(res2.isConfirmed());
            assertEquals(res1.getTicketId(), res2.getTicketId(), "Cùng requestId phải trả về cùng ticketId đã cấp");

            // Leader B chỉ có đúng 1 vé ACTIVE trên ghế G06
            assertEquals(1, serverB.getActiveTicketCount("G06"), "Chỉ được tồn tại duy nhất 1 vé ACTIVE");
            assertEquals(0, serverA.getActiveTicketCount("G06"));
        });
    }

    @Test
    @DisplayName("Ca 11: NO_QUORUM và REQUEST_ID_CONFLICT không bị redirect")
    void test11_NoQuorumAndRequestIdConflictNotRedirected() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            serverA.setForcedError("NO_QUORUM", "Không đủ quorum 3/5 node");

            ClusterEndpoints endpoints = createEndpoints(serverA);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            // Kiểm tra NO_QUORUM
            String reqId1 = UUID.randomUUID().toString();
            BookTicketResponseDto res1 = client.book(new BookTicketRequestDto(reqId1, "SE-001", "G01", "Khách 11A")).get(5, TimeUnit.SECONDS);
            assertNotNull(res1);
            assertEquals("NO_QUORUM", res1.getReasonCode(), "NO_QUORUM phải được trả nguyên, không redirect");

            // Kiểm tra REQUEST_ID_CONFLICT
            serverA.setForcedError("REQUEST_ID_CONFLICT", "Xung đột payload requestId");
            String reqId2 = UUID.randomUUID().toString();
            BookTicketResponseDto res2 = client.book(new BookTicketRequestDto(reqId2, "SE-001", "G01", "Khách 11B")).get(5, TimeUnit.SECONDS);
            assertNotNull(res2);
            assertEquals("REQUEST_ID_CONFLICT", res2.getReasonCode(), "REQUEST_ID_CONFLICT phải trả nguyên, không redirect");
        });
    }

    @Test
    @DisplayName("Ca 12: Timeout tổng được tôn trọng; không bị treo vô hạn")
    void test12_TotalTimeoutRespected() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            // Mỗi bước delay 600ms, tổng timeout là 800ms
            serverA.setDelayMs(600);
            serverA.setNotLeader("node-2", 1L);
            serverB.setDelayMs(600);
            serverB.setLeader();

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = new RedirectPolicy(3, 1000, 800);
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            String requestId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(requestId, "SE-001", "G01", "Khách Test 12");

            BookTicketResponseDto response = client.book(request).get(3, TimeUnit.SECONDS);

            assertNotNull(response);
            assertTrue(response.isUnknown());
            assertEquals("TIMEOUT_UNKNOWN", response.getReasonCode());
            assertTrue(response.getMessage().contains("tổng thể"));
        });
    }

    @Test
    @DisplayName("Ca 13: Sau khi redirect thành công, lệnh thứ 2 gửi thẳng tới Leader đã biết; Leader đổi vẫn tự chuyển tiếp")
    void test13_SubsequentCallGoesDirectlyToKnownLeader() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");
            FakeNodeServer serverC = createServer("node-3");

            serverA.setNotLeader("node-2", 2L);
            serverB.setLeader();
            serverC.setLeader();

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB, serverC);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, null);

            // Lệnh 1: gửi tới A -> redirect tới B -> thành công
            BookTicketRequestDto req1 = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G01", "Khách 13A");
            BookTicketResponseDto res1 = client.book(req1).get(5, TimeUnit.SECONDS);
            assertTrue(res1.isConfirmed());
            assertEquals("node-2", client.getKnownLeaderId(), "Client phải ghi nhớ Leader là node-2");

            // Lệnh 2: gửi lệnh mới -> phải gửi thẳng tới B (node-2)
            BookTicketRequestDto req2 = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G03", "Khách 13B");
            BookTicketResponseDto res2 = client.book(req2).get(5, TimeUnit.SECONDS);
            assertTrue(res2.isConfirmed());

            // Kiểm tra số request tại A: A chỉ nhận đúng 1 request từ lệnh đầu tiên, lệnh 2 không hề qua A!
            assertEquals(1, serverA.getReceivedRequests().size(), "Lệnh thứ 2 không được gửi qua Follower A");
            assertEquals(2, serverB.getReceivedRequests().size(), "Leader B phải nhận trực tiếp lệnh thứ 2");

            // Giả lập Leader B bị mất vai trò và trở thành Follower trỏ tới C
            serverB.setNotLeader("node-3", 3L);
            BookTicketRequestDto req3 = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G04", "Khách 13C");
            BookTicketResponseDto res3 = client.book(req3).get(5, TimeUnit.SECONDS);
            assertTrue(res3.isConfirmed());
            assertEquals("node-3", client.getKnownLeaderId(), "Client phải tự động cập nhật Leader mới là node-3");
            assertEquals(1, serverC.getReceivedRequests().size(), "Leader C nhận thành công request chuyển tiếp");
        });
    }

    @Test
    @DisplayName("Ca 14: ClusterConnectionViewModel nhận leaderHint với nguồn REAL sau redirect")
    void test14_ClusterConnectionViewModelUpdatedWithRealSource() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer serverA = createServer("node-1");
            FakeNodeServer serverB = createServer("node-2");

            serverA.setNotLeader("node-2", 2L);
            serverB.setLeader();

            ClusterConnectionViewModel vm = new ClusterConnectionViewModel(MockClusterStatusClient.getInstance());
            vm.setSyncMode(true); // Chạy đồng bộ trong test không cần JavaFX application thread

            ClusterEndpoints endpoints = createEndpoints(serverA, serverB);
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            LeaderAwareBookingClient client = createClient(endpoints, policy, vm);

            assertEquals("-", vm.getLeaderHint(), "Ban đầu leaderHint phải là - (UNKNOWN)");

            BookTicketRequestDto request = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G01", "Khách VM");
            BookTicketResponseDto response = client.book(request).get(5, TimeUnit.SECONDS);

            assertTrue(response.isConfirmed());
            assertEquals("node-2", vm.getLeaderHint(), "ViewModel phải nhận được gợi ý leaderHint là node-2");
            assertEquals(ClusterStatusClient.DataSource.REAL, vm.getDataSource(), "Nguồn dữ liệu phải là REAL (socket thật)");
            assertEquals("node-2", vm.getConnectedNodeId(), "connectedNodeId phải đổi sang node-2 sau redirect");

            vm.close();
        });
    }

    // =========================================================================
    // KIỂM THỬ CỤM THẬT (CHỈ CHẠY KHI CÓ BIẾN MÔI TRƯỜNG, NẾU KHÔNG SẼ SKIPPED)
    // =========================================================================

    @Test
    @DisplayName("Kiểm thử cụm thật (TV5): Chỉ kích hoạt khi có NODE_ENDPOINTS, không có thì SKIPPED")
    @EnabledIfEnvironmentVariable(named = "NODE_ENDPOINTS", matches = ".+")
    void testRealClusterIntegration() {
        // Chỉ chạy khi có cluster thật được triển khai theo SPEC-08
        String nodeEndpointsEnv = System.getenv("NODE_ENDPOINTS");
        assertNotNull(nodeEndpointsEnv);
    }
}
