package vn.nhom6.railway.client.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.SeatsRefreshService;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.VerificationLevel;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test harness 6 Client tranh chấp đồng thời 1 ghế trên FAKE SERVER (TASK TV1-D18).
 * <p>
 * <b>LƯU Ý BẮT BUỘC:</b>
 * <ul>
 *   <li>Mọi ca test mặc định chạy với {@link FakeNodeServer} (<b>FAKE SERVER, không phải Node thật, không phải Raft</b>).</li>
 *   <li>Kết quả PASS tại đây chỉ chứng minh luồng UI/ViewModel/Journal/TCP protocol phía Desktop Client.</li>
 *   <li>AC03 thật (6 Client trên cụm 5 Node Raft ordered apply) KHÔNG được nghiệm thu tại đây mà tại mốc G5 sau TV4-D18.</li>
 *   <li>Sử dụng {@link BookingViewModel.UiDispatcher} đồng bộ ({@code Runnable::run}) nên chạy hoàn toàn headless,
 *       không phụ thuộc JavaFX toolkit hay mạng ngoài.</li>
 * </ul>
 */
@DisplayName("TV1-D18: Harness 6 Client tranh chấp cùng ghế (FAKE SERVER, không phải Node thật)")
class SixClientContentionTest {

    private static final int CLIENT_COUNT = 6;
    private static final String TRIP_ID = "SE-001";
    private static final String TARGET_SEAT = "G02";

    private FakeNodeServer fakeServer;

    @BeforeEach
    void setUp() throws IOException {
        // FAKE SERVER, không phải Node thật, không phải Raft
        fakeServer = new FakeNodeServer("node-1", 0);
        fakeServer.setLeader();
    }

    @AfterEach
    void tearDown() {
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    // =========================================================================
    // KỊCH BẢN CHÍNH (Tiêu chí 1, 2, 3, 4, 5, 6)
    // =========================================================================

    @Test
    @DisplayName("1-6. 6 Client đặt đồng thời G02 -> đúng 1 CONFIRMED, 5 REJECTED(SEAT_ALREADY_BOOKED), refresh UNVERIFIED, journal sạch")
    void testSixClientsSimultaneousContention_AllCoreInvariants(@TempDir Path tempDir) throws Exception {
        fakeServer.configureRandomDelay(42L, 25);

        List<ClientStack> stacks = createSixClientStacks(tempDir, 2000, 3000, 5000, false);
        try {
            Map<Integer, String> requestIds = new HashMap<>();
            for (ClientStack stack : stacks) {
                requestIds.put(stack.clientIndex, UUID.randomUUID().toString());
            }

            // (2) 6 requestId đôi một khác nhau
            assertEquals(CLIENT_COUNT, new HashSet<>(requestIds.values()).size(),
                    "6 Client phải sử dụng 6 requestId đôi một khác nhau");

            // Barrier đồng bộ cả 6 submit cùng thời điểm
            Map<Integer, CompletableFuture<BookTicketResponseDto>> futures =
                    submitAllSimultaneously(stacks, requestIds, TARGET_SEAT);

            // Chờ cả 6 hoàn thành phản hồi cuối
            for (ClientStack stack : stacks) {
                BookTicketResponseDto resp = futures.get(stack.clientIndex).get(8, TimeUnit.SECONDS);
                assertNotNull(resp, "Client-" + stack.clientIndex + " phải nhận được phản hồi");
            }

            // (1) Đếm CONFIRMED và REJECTED(SEAT_ALREADY_BOOKED)
            int confirmedCount = 0;
            int rejectedAlreadyBookedCount = 0;
            int winnerClient = -1;

            for (ClientStack stack : stacks) {
                BookingViewModel vm = stack.viewModel;
                String expectedReqId = requestIds.get(stack.clientIndex);

                // (2) requestId không bị đổi giữa chừng
                assertEquals(expectedReqId, vm.getCurrentRequestId(),
                        "requestId của client-" + stack.clientIndex + " không được đổi giữa chừng");

                // (3) Mọi Client thoát SUBMITTING và busy = false
                assertNotEquals(BookingViewModel.UiState.SUBMITTING, vm.getUiState(),
                        "Client-" + stack.clientIndex + " không được kẹt ở SUBMITTING");
                assertFalse(vm.isBusy(), "Client-" + stack.clientIndex + " phải có busy = false");

                // (5) Đối chiếu nghiêm ngặt với quyết định trên FAKE SERVER theo đúng requestId
                FakeNodeServer.StoredBooking serverDecision = fakeServer.getStoredBooking(expectedReqId);
                assertNotNull(serverDecision, "FAKE SERVER phải lưu quyết định cho requestId=" + expectedReqId);

                if (vm.getUiState() == BookingViewModel.UiState.CONFIRMED) {
                    confirmedCount++;
                    winnerClient = stack.clientIndex;
                    assertEquals("CONFIRMED", serverDecision.responseDto().getOutcome(),
                            "Client chỉ được hiển thị CONFIRMED khi server trả CONFIRMED cho chính requestId đó");
                } else if (vm.getUiState() == BookingViewModel.UiState.REJECTED) {
                    rejectedAlreadyBookedCount++;
                    assertEquals(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, vm.getErrorCode(),
                            "Client bị từ chối phải mang mã SEAT_ALREADY_BOOKED");
                    assertEquals("REJECTED", serverDecision.responseDto().getOutcome(),
                            "Client REJECTED phải khớp server REJECTED");
                }
            }

            assertEquals(1, confirmedCount, "Phải có ĐÚNG 1 Client CONFIRMED trên FAKE SERVER");
            assertEquals(CLIENT_COUNT - 1, rejectedAlreadyBookedCount,
                    "Phải có ĐÚNG 5 Client REJECTED(SEAT_ALREADY_BOOKED) trên FAKE SERVER");
            assertTrue(winnerClient >= 1 && winnerClient <= CLIENT_COUNT);
            assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT),
                    "FAKE SERVER phải có ĐÚNG 1 vé ACTIVE cho ghế " + TARGET_SEAT);

            // (2) Kiểm tra frame nhận được trên FAKE SERVER: mỗi requestId đi đúng từ senderId của chính Client đó
            List<MessageEnvelope<JsonNode>> receivedBookFrames = fakeServer.getReceivedRequests().stream()
                    .filter(env -> env.getType() == MessageType.BOOK_TICKET)
                    .toList();
            assertEquals(CLIENT_COUNT, receivedBookFrames.size(), "FAKE SERVER phải nhận đúng 6 khung BOOK_TICKET");
            for (ClientStack stack : stacks) {
                String expectedSender = "client-" + stack.clientIndex;
                String expectedReqId = requestIds.get(stack.clientIndex);
                boolean matched = receivedBookFrames.stream().anyMatch(env ->
                        expectedSender.equals(env.getSenderId())
                                && env.getPayload() != null
                                && expectedReqId.equals(env.getPayload().path("requestId").asText()));
                assertTrue(matched, "Frame BOOK_TICKET của " + expectedSender + " phải chứa đúng requestId=" + expectedReqId);
            }

            // (4) Refresh ghế ở cả 6 Client: G02 là BOOKED, kèm nhãn "Chưa xác minh" (D15), ghế khác vẫn AVAILABLE
            for (ClientStack stack : stacks) {
                stack.seatsRefreshService.refreshNow().get(5, TimeUnit.SECONDS);

                assertEquals(VerificationLevel.UNVERIFIED, stack.seatsRefreshService.getVerification(),
                        "Sau refresh, dữ liệu ghế phải luôn ở mức UNVERIFIED (Chưa xác minh theo D15)");
                assertEquals("Chưa xác minh", stack.seatsRefreshService.getVerification().getDisplayName());

                for (SeatViewModel seat : stack.seatsList) {
                    if (TARGET_SEAT.equals(seat.getSeatId())) {
                        assertEquals(SeatViewModel.SeatStatus.BOOKED, seat.getStatus(),
                                "Ghế " + TARGET_SEAT + " phải là BOOKED trên client-" + stack.clientIndex);
                    } else {
                        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, seat.getStatus(),
                                "Ghế khác (" + seat.getSeatId() + ") phải giữ nguyên AVAILABLE trên client-" + stack.clientIndex);
                    }
                }
            }

            // (6) Không còn pending nào trong journal của cả 6 Client
            for (ClientStack stack : stacks) {
                List<PendingEntry> remainingPending = stack.journal.loadPending();
                assertTrue(remainingPending.isEmpty(),
                        "Journal của client-" + stack.clientIndex + " không được còn pending sau khi nhận kết quả cuối");
            }
        } finally {
            closeAll(stacks);
        }
    }

    // =========================================================================
    // TIÊU CHÍ 7: LẶP 20 VÒNG VỚI SEED KHÁC NHAU
    // =========================================================================

    @Test
    @DisplayName("7. Lặp 20 vòng với seed khác nhau -> mọi vòng luôn đúng 1 CONFIRMED, 1 vé ACTIVE, người thắng đa dạng")
    void testTwentyRoundsWithDifferentSeeds_AlwaysExactlyOneWinner(@TempDir Path tempDir) throws Exception {
        final int rounds = 20;
        Set<Integer> distinctWinners = new HashSet<>();

        List<ClientStack> stacks = createSixClientStacks(tempDir, 2000, 3000, 5000, false);
        try {
            for (int round = 1; round <= rounds; round++) {
                fakeServer.resetForNewRound();
                fakeServer.configureRandomDelay(1000L + round * 37L, 30);

                Map<Integer, String> requestIds = new HashMap<>();
                for (ClientStack stack : stacks) {
                    requestIds.put(stack.clientIndex, UUID.randomUUID().toString());
                }

                Map<Integer, CompletableFuture<BookTicketResponseDto>> futures =
                        submitAllSimultaneously(stacks, requestIds, TARGET_SEAT);

                int confirmed = 0;
                int rejected = 0;
                int roundWinner = -1;

                for (ClientStack stack : stacks) {
                    futures.get(stack.clientIndex).get(8, TimeUnit.SECONDS);
                    assertFalse(stack.viewModel.isBusy(), "Vòng " + round + ": client-" + stack.clientIndex + " bị kẹt busy");

                    if (stack.viewModel.getUiState() == BookingViewModel.UiState.CONFIRMED) {
                        confirmed++;
                        roundWinner = stack.clientIndex;
                    } else if (stack.viewModel.getUiState() == BookingViewModel.UiState.REJECTED
                            && ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED.equals(stack.viewModel.getErrorCode())) {
                        rejected++;
                    }
                }

                assertEquals(1, confirmed, "Vòng " + round + " phải có ĐÚNG 1 CONFIRMED");
                assertEquals(CLIENT_COUNT - 1, rejected, "Vòng " + round + " phải có ĐÚNG 5 REJECTED(SEAT_ALREADY_BOOKED)");
                assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT),
                        "Vòng " + round + ": FAKE SERVER phải có ĐÚNG 1 vé ACTIVE cho " + TARGET_SEAT);
                distinctWinners.add(roundWinner);
            }

            assertTrue(distinctWinners.size() >= 2,
                    "Qua 20 vòng với seed/jitter khác nhau, Client thắng nên thay đổi (quan sát được: " + distinctWinners + ")");
        } finally {
            closeAll(stacks);
        }
    }

    // =========================================================================
    // TIÊU CHÍ 8: KỊCH BẢN UNKNOWN (RƠI PHẢN HỒI) + GET_REQUEST_RESULT
    // =========================================================================

    @Test
    @DisplayName("8. Kịch bản UNKNOWN: 1 Client bị rơi phản hồi -> UNKNOWN + còn pending -> getRequestResult cùng requestId -> RESOLVED, tổng 1 vé ACTIVE")
    void testUnknownScenario_DroppedResponseThenGetRequestResultReconciles(@TempDir Path tempDir) throws Exception {
        final int droppedClientIdx = 3;
        final String droppedSenderId = "client-" + droppedClientIdx;

        // FAKE SERVER: xử lý & lưu kết quả bình thường nhưng làm rơi phản hồi cho client-3
        fakeServer.configureRandomDelay(77L, 15);
        fakeServer.addDropResponseForSender(droppedSenderId);
        fakeServer.setDropResponseHoldMs(4000L);

        // Dùng timeout ngắn cho client để test chạy nhanh, và bật Spec03LookupAdapter (test-only)
        // cho GET_REQUEST_RESULT SPEC-03 §4.4 (vì src/main SocketBookingClient.getRequestResult đang BLOCKED chờ DTO TV3)
        List<ClientStack> stacks = createSixClientStacks(tempDir, 1000, 600, 450, true);
        try {
            Map<Integer, String> requestIds = new HashMap<>();
            for (ClientStack stack : stacks) {
                requestIds.put(stack.clientIndex, UUID.randomUUID().toString());
            }

            Map<Integer, CompletableFuture<BookTicketResponseDto>> futures =
                    submitAllSimultaneously(stacks, requestIds, TARGET_SEAT);

            // Chờ 5 Client không bị rơi nhận xong phản hồi
            for (ClientStack stack : stacks) {
                if (stack.clientIndex != droppedClientIdx) {
                    futures.get(stack.clientIndex).get(5, TimeUnit.SECONDS);
                    assertFalse(stack.viewModel.isBusy(), "Client-" + stack.clientIndex + " phải hoàn thành");
                }
            }

            // Chờ Client bị rơi phản hồi chuyển sang UNKNOWN (do timeout UI hoặc read timeout)
            ClientStack droppedStack = stacks.get(droppedClientIdx - 1);
            String droppedReqId = requestIds.get(droppedClientIdx);
            waitUntil(() -> droppedStack.viewModel.getUiState() == BookingViewModel.UiState.UNKNOWN
                    && !droppedStack.viewModel.isBusy(), 4000);

            assertEquals(BookingViewModel.UiState.UNKNOWN, droppedStack.viewModel.getUiState(),
                    "Client bị rơi phản hồi phải chuyển sang UNKNOWN (tuyệt đối không biến thành REJECTED)");
            assertEquals(droppedReqId, droppedStack.viewModel.getCurrentRequestId(),
                    "requestId của Client UNKNOWN phải được giữ nguyên");

            // Pending vẫn còn trong journal của client-3
            List<PendingEntry> pendingBeforeLookup = droppedStack.journal.loadPending();
            assertEquals(1, pendingBeforeLookup.size(),
                    "Client bị rơi phản hồi phải còn đúng 1 bản ghi pending chưa giải quyết trong journal");
            assertEquals(droppedReqId, pendingBeforeLookup.get(0).getRequestId());

            // Tổng số vé ACTIVE trên FAKE SERVER ngay lúc này vẫn phải là 1
            assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT),
                    "Tổng vé ACTIVE cho " + TARGET_SEAT + " trên FAKE SERVER phải là 1");

            // Đối chiếu: kiểm tra kết quả lưu trên server cho client-3 (có thể là CONFIRMED hoặc REJECTED tùy thứ tự đến)
            FakeNodeServer.StoredBooking storedForDropped = fakeServer.getStoredBooking(droppedReqId);
            assertNotNull(storedForDropped, "FAKE SERVER phải đã lưu quyết định cho " + droppedSenderId);
            String expectedStoredOutcome = storedForDropped.responseDto().getOutcome();

            // Thực hiện GET_REQUEST_RESULT cùng requestId (SPEC-03 §4.4)
            GetRequestResultResponseDto lookupResult = droppedStack.viewModel.checkCurrentResult().get(5, TimeUnit.SECONDS);
            assertNotNull(lookupResult);
            assertEquals(droppedReqId, lookupResult.getRequestId());

            if ("CONFIRMED".equals(expectedStoredOutcome)) {
                assertEquals("APPLIED_SUCCESS", lookupResult.getState());
                assertEquals(BookingViewModel.UiState.CONFIRMED, droppedStack.viewModel.getUiState());
            } else {
                assertEquals("APPLIED_REJECTED", lookupResult.getState());
                assertEquals(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, lookupResult.getReasonCode());
                assertEquals(BookingViewModel.UiState.REJECTED, droppedStack.viewModel.getUiState());
            }

            // Sau khi tra cứu xong, journal của client-3 được RESOLVED (không còn pending)
            assertTrue(droppedStack.journal.loadPending().isEmpty(),
                    "Sau getRequestResult nhận kết quả cuối, pending của client-3 phải được RESOLVED");

            // Tổng kết toàn bộ 6 Client sau đối soát: ĐÚNG 1 CONFIRMED, 5 REJECTED, tổng vé ACTIVE = 1
            long finalConfirmed = stacks.stream()
                    .filter(s -> s.viewModel.getUiState() == BookingViewModel.UiState.CONFIRMED).count();
            long finalRejected = stacks.stream()
                    .filter(s -> s.viewModel.getUiState() == BookingViewModel.UiState.REJECTED).count();
            assertEquals(1, finalConfirmed, "Sau đối soát phải có đúng 1 Client CONFIRMED");
            assertEquals(CLIENT_COUNT - 1, finalRejected, "Sau đối soát phải có đúng 5 Client REJECTED");
            assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT),
                    "Tổng số vé ACTIVE trên FAKE SERVER sau đối soát vẫn phải là 1");
        } finally {
            closeAll(stacks);
        }
    }

    // =========================================================================
    // TIÊU CHÍ 9: RETRY CÙNG REQUEST_ID KHÔNG TẠO VÉ THỨ HAI
    // =========================================================================

    @Test
    @DisplayName("9. Retry cùng requestId từ Client không tạo vé thứ hai và giữ nguyên kết quả ban đầu")
    void testRetrySameRequestId_DoesNotCreateSecondTicket(@TempDir Path tempDir) throws Exception {
        List<ClientStack> stacks = createSixClientStacks(tempDir, 2000, 3000, 5000, false);
        try {
            Map<Integer, String> requestIds = new HashMap<>();
            for (ClientStack stack : stacks) {
                requestIds.put(stack.clientIndex, UUID.randomUUID().toString());
            }

            Map<Integer, CompletableFuture<BookTicketResponseDto>> futures =
                    submitAllSimultaneously(stacks, requestIds, TARGET_SEAT);

            ClientStack winner = null;
            ClientStack loser = null;
            BookTicketResponseDto winnerFirstResp = null;

            for (ClientStack stack : stacks) {
                BookTicketResponseDto resp = futures.get(stack.clientIndex).get(8, TimeUnit.SECONDS);
                if (stack.viewModel.getUiState() == BookingViewModel.UiState.CONFIRMED) {
                    winner = stack;
                    winnerFirstResp = resp;
                } else if (loser == null && stack.viewModel.getUiState() == BookingViewModel.UiState.REJECTED) {
                    loser = stack;
                }
            }

            assertNotNull(winner, "Phải có 1 Client thắng");
            assertNotNull(loser, "Phải có ít nhất 1 Client bị từ chối");
            assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT));

            // Gửi lại (retry) CÙNG requestId + CÙNG payload từ Client thắng
            String winnerReqId = requestIds.get(winner.clientIndex);
            BookTicketRequestDto winnerRetryReq = new BookTicketRequestDto(
                    winnerReqId, TRIP_ID, TARGET_SEAT, "Hành khách Test " + winner.clientIndex);
            BookTicketResponseDto winnerRetryResp = winner.rawBookingClient.book(winnerRetryReq).get(5, TimeUnit.SECONDS);

            assertEquals("CONFIRMED", winnerRetryResp.getOutcome(), "Retry cùng requestId của người thắng phải trả CONFIRMED");
            assertEquals(winnerFirstResp.getTicketId(), winnerRetryResp.getTicketId(),
                    "Retry cùng requestId phải trả về CÙNG ticketId ban đầu");
            assertEquals(winnerFirstResp.getAppliedLogIndex(), winnerRetryResp.getAppliedLogIndex(),
                    "Retry cùng requestId phải giữ nguyên appliedLogIndex gốc");
            assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT),
                    "Retry cùng requestId tuyệt đối KHÔNG được tạo vé ACTIVE thứ hai");
            assertEquals(2, fakeServer.getRequestAttemptCount(winnerReqId),
                    "FAKE SERVER ghi nhận 2 lượt gửi của cùng requestId nhưng chỉ 1 vé ACTIVE");

            // Gửi lại cùng requestId từ Client bị từ chối -> vẫn REJECTED(SEAT_ALREADY_BOOKED)
            String loserReqId = requestIds.get(loser.clientIndex);
            BookTicketRequestDto loserRetryReq = new BookTicketRequestDto(
                    loserReqId, TRIP_ID, TARGET_SEAT, "Hành khách Test " + loser.clientIndex);
            BookTicketResponseDto loserRetryResp = loser.rawBookingClient.book(loserRetryReq).get(5, TimeUnit.SECONDS);
            assertEquals("REJECTED", loserRetryResp.getOutcome());
            assertEquals(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, loserRetryResp.getReasonCode());
            assertEquals(1, fakeServer.getActiveTicketCount(TARGET_SEAT));
        } finally {
            closeAll(stacks);
        }
    }

    // =========================================================================
    // KỊCH BẢN RAFT THẬT (MẶC ĐỊNH SKIPPED — NGHIỆM THU TẠI G5 SAU TV4-D18)
    // =========================================================================

    @Test
    @EnabledIfEnvironmentVariable(named = "CLUSTER_PROFILE", matches = "real")
    @DisplayName("AC03 trên cụm 5 Node Raft thật (SKIPPED mặc định — nghiệm thu tại G5 sau TV4-D18)")
    void testRealRaftClusterAC03_SkippedUntilG5() {
        String endpointsEnv = System.getenv("NODE_ENDPOINTS");
        assertNotNull(endpointsEnv, "Cần biến môi trường NODE_ENDPOINTS khi chạy kiểm thử cụm Raft thật tại G5");
    }

    // =========================================================================
    // HELPERS & TEST-ONLY SPEC-03 GET_REQUEST_RESULT ADAPTER
    // =========================================================================

    private List<ClientStack> createSixClientStacks(
            Path tempDir,
            int connectTimeoutMs,
            int readTimeoutMs,
            int submitTimeoutMs,
            boolean enableSpec03LookupAdapter) throws IOException {

        List<ClientStack> stacks = new ArrayList<>();
        NodeEndpoint fakeEndpoint = new NodeEndpoint("node-1", "127.0.0.1", fakeServer.getPort());
        ClusterEndpoints clusterEndpoints = ClusterEndpoints.fromEndpoints(List.of(fakeEndpoint), "node-1");
        RedirectPolicy policy = new RedirectPolicy(2, connectTimeoutMs, readTimeoutMs);

        for (int i = 1; i <= CLIENT_COUNT; i++) {
            final int clientIdx = i;
            final String senderId = "client-" + clientIdx;
            Path journalDir = tempDir.resolve("client-" + clientIdx);
            PersistentPendingJournal journal = new PersistentPendingJournal(journalDir);

            LeaderAwareBookingClient leaderAware = new LeaderAwareBookingClient(
                    clusterEndpoints,
                    ep -> {
                        SocketBookingClient socketClient = new SocketBookingClient(
                                ep.host(), ep.port(), connectTimeoutMs, readTimeoutMs, senderId);
                        if (enableSpec03LookupAdapter) {
                            return new TestSpec03LookupClientAdapter(socketClient, ep.host(), ep.port(),
                                    connectTimeoutMs, readTimeoutMs, senderId);
                        }
                        return socketClient;
                    },
                    policy,
                    null,
                    "node-1",
                    null
            );

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderAware, journal);

            ObservableList<SeatViewModel> seats = FXCollections.observableArrayList();
            for (int s = 1; s <= 10; s++) {
                seats.add(new SeatViewModel(String.format("G%02d", s), SeatViewModel.SeatStatus.AVAILABLE));
            }

            BookingClientReadAdapter readAdapter = new BookingClientReadAdapter(leaderAware);
            SeatsRefreshService refreshService = new SeatsRefreshService(readAdapter, seats, journal);

            java.util.concurrent.ScheduledExecutorService vmScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "d18-vm-timer-client-" + clientIdx);
                t.setDaemon(true);
                return t;
            });

            // UiDispatcher đồng bộ (D17) để chạy headless không phụ thuộc JavaFX toolkit
            BookingViewModel vm = new BookingViewModel(journalingClient, journal, refreshService, vmScheduler, submitTimeoutMs);
            vm.setUiDispatcher(Runnable::run);
            vm.setSeatsList(seats);

            stacks.add(new ClientStack(clientIdx, journal, leaderAware, journalingClient, vm, vmScheduler, seats, refreshService));
        }

        return stacks;
    }

    private Map<Integer, CompletableFuture<BookTicketResponseDto>> submitAllSimultaneously(
            List<ClientStack> stacks,
            Map<Integer, String> requestIds,
            String seatId) throws InterruptedException {

        CountDownLatch readyLatch = new CountDownLatch(stacks.size());
        CountDownLatch startGate = new CountDownLatch(1);
        Map<Integer, CompletableFuture<BookTicketResponseDto>> futures = new HashMap<>();
        Object mapLock = new Object();

        ExecutorService pool = Executors.newFixedThreadPool(stacks.size());
        try {
            for (ClientStack stack : stacks) {
                pool.submit(() -> {
                    String reqId = requestIds.get(stack.clientIndex);
                    BookTicketRequestDto req = new BookTicketRequestDto(
                            reqId, TRIP_ID, seatId, "Hành khách Test " + stack.clientIndex);
                    readyLatch.countDown();
                    try {
                        startGate.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    CompletableFuture<BookTicketResponseDto> f = stack.viewModel.submit(req);
                    synchronized (mapLock) {
                        futures.put(stack.clientIndex, f);
                    }
                });
            }

            assertTrue(readyLatch.await(5, TimeUnit.SECONDS), "Cả 6 luồng phải sẵn sàng tại barrier");
            startGate.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "Cả 6 lời gọi submit phải được phát đi");
        } finally {
            pool.shutdownNow();
        }
        return futures;
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25L);
        }
    }

    private static void closeAll(List<ClientStack> stacks) {
        for (ClientStack stack : stacks) {
            stack.close();
        }
    }

    private static final class ClientStack implements AutoCloseable {
        final int clientIndex;
        final PersistentPendingJournal journal;
        final LeaderAwareBookingClient rawBookingClient;
        final JournalingBookingClient journalingClient;
        final BookingViewModel viewModel;
        final java.util.concurrent.ScheduledExecutorService vmScheduler;
        final ObservableList<SeatViewModel> seatsList;
        final SeatsRefreshService seatsRefreshService;

        ClientStack(
                int clientIndex,
                PersistentPendingJournal journal,
                LeaderAwareBookingClient rawBookingClient,
                JournalingBookingClient journalingClient,
                BookingViewModel viewModel,
                java.util.concurrent.ScheduledExecutorService vmScheduler,
                ObservableList<SeatViewModel> seatsList,
                SeatsRefreshService seatsRefreshService) {
            this.clientIndex = clientIndex;
            this.journal = journal;
            this.rawBookingClient = rawBookingClient;
            this.journalingClient = journalingClient;
            this.viewModel = viewModel;
            this.vmScheduler = vmScheduler;
            this.seatsList = seatsList;
            this.seatsRefreshService = seatsRefreshService;
        }

        @Override
        public void close() {
            try {
                seatsRefreshService.close();
            } catch (Exception ignored) {
            }
            try {
                vmScheduler.shutdownNow();
            } catch (Exception ignored) {
            }
            try {
                rawBookingClient.close();
            } catch (Exception ignored) {
            }
            try {
                journal.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Test-only adapter bọc {@link SocketBookingClient} cho riêng kịch bản UNKNOWN của D18.
     * <p>
     * Lý do: {@code SocketBookingClient.getRequestResult()} ở {@code src/main} đang giữ nguyên
     * trạng thái {@code BLOCKED} do TV3 chưa bàn giao DTO {@code GetRequestResultRequestDto} trong module {@code shared}.
     * Adapter này chỉ nằm trong test sources để xác minh luồng wire SPEC-03 §4.4 với {@link FakeNodeServer}
     * mà không sửa {@code shared} và không làm hỏng test D09/D10.
     */
    private static final class TestSpec03LookupClientAdapter implements BookingClient, AutoCloseable {
        private final SocketBookingClient delegate;
        private final String host;
        private final int port;
        private final int connectTimeoutMs;
        private final int readTimeoutMs;
        private final String senderId;

        TestSpec03LookupClientAdapter(
                SocketBookingClient delegate,
                String host,
                int port,
                int connectTimeoutMs,
                int readTimeoutMs,
                String senderId) {
            this.delegate = delegate;
            this.host = host;
            this.port = port;
            this.connectTimeoutMs = connectTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
            this.senderId = senderId;
        }

        @Override
        public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
            return delegate.getSeats(request);
        }

        @Override
        public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
            return delegate.book(request);
        }

        @Override
        public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
            return delegate.cancel(request);
        }

        @Override
        public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
            return CompletableFuture.supplyAsync(() -> {
                try (Socket socket = new Socket()) {
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(readTimeoutMs);
                    socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);

                    ObjectNode payload = MessageCodec.getMapper().createObjectNode();
                    payload.put("requestId", requestId);
                    MessageEnvelope<ObjectNode> reqEnv =
                            MessageEnvelope.createRequest(MessageType.GET_REQUEST_RESULT, senderId, payload);

                    OutputStream out = new BufferedOutputStream(socket.getOutputStream());
                    InputStream in = socket.getInputStream();
                    FrameCodec.writeMessage(out, reqEnv);

                    byte[] respBytes = FrameCodec.readFrame(in);
                    MessageEnvelope<JsonNode> respEnv = MessageCodec.decode(respBytes);
                    JsonNode p = respEnv.getPayload();

                    String state = p.hasNonNull("state") ? p.get("state").asText() : "UNKNOWN";
                    String commandType = p.hasNonNull("commandType") ? p.get("commandType").asText() : null;
                    String ticketId = p.hasNonNull("ticketId") ? p.get("ticketId").asText() : null;
                    String reasonCode = p.hasNonNull("reasonCode") ? p.get("reasonCode").asText() : null;
                    Long appliedLogIndex = p.hasNonNull("appliedLogIndex") ? p.get("appliedLogIndex").asLong() : null;

                    return new GetRequestResultResponseDto(
                            requestId, state, commandType, ticketId, reasonCode, appliedLogIndex, false);
                } catch (Exception e) {
                    throw new RuntimeException("Lỗi tra cứu GET_REQUEST_RESULT trên FAKE SERVER: " + e.getMessage(), e);
                }
            });
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
