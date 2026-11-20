package vn.nhom6.railway.client;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.ClientErrorKind;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SeatsSnapshot;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử chấp nhận giao diện người dùng độc lập (UI Acceptance Case Suite)
 * trước mốc G5 (TASK TV1-D20).
 * <p>
 * <b>BẢN QUYỀN VÀ RÀNG BUỘC KIỂM THỬ:</b>
 * <ul>
 *   <li>Mọi test trong file này sử dụng fixture/fake server trong localhost (cổng ngẫu nhiên).</li>
 *   <li><b>FAKE:</b> quorum, Leader, commit/apply đều do fixture GIẢ LẬP, không phải Raft thật.</li>
 *   <li>Không tuyên bố bất kỳ AC phân tán nào là "PASS thật" từ kết quả fixture này.</li>
 *   <li>Cụm 5 Node thật (ports 9101–9105) được bảo vệ bằng {@link Assumptions#assumeTrue}, mặc định SKIPPED khi offline.</li>
 * </ul>
 */
@Timeout(value = 25, unit = TimeUnit.SECONDS)
public class BookingUiAcceptanceTest {

    @TempDir
    Path tempJournalDir;

    private final List<FakeNodeServer> serversToClose = new ArrayList<>();
    private final List<AutoCloseable> clientsToClose = new ArrayList<>();

    private PersistentPendingJournal journal;
    private ObservableList<SeatViewModel> seatsList;
    private TrackingRefreshService refreshService;
    private AtomicInteger uiDispatchCount;

    static class TrackingRefreshService extends SeatsRefreshService {
        final AtomicInteger refreshCount = new AtomicInteger(0);

        TrackingRefreshService(ObservableList<SeatViewModel> list, PersistentPendingJournal journal) {
            super(new FakeReadAdapter(), list, journal);
        }

        @Override
        public CompletableFuture<SeatsSnapshot> refreshNow() {
            refreshCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Client adapter hỗ trợ SocketBookingClient và mở rộng gửi GET_REQUEST_RESULT qua TCP thật
     * tới FakeNodeServer theo đúng đặc tả SPEC-03 Section 4.4.
     */
    static class AcceptanceSocketBookingClient implements BookingClient, AutoCloseable {
        private final SocketBookingClient delegate;
        private final String host;
        private final int port;

        AcceptanceSocketBookingClient(String host, int port) {
            this(host, port, 2000, 3000);
        }

        AcceptanceSocketBookingClient(String host, int port, int connectTimeoutMs, int readTimeoutMs) {
            this.host = host;
            this.port = port;
            this.delegate = new SocketBookingClient(host, port, connectTimeoutMs, readTimeoutMs, "client-acc-test");
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
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress(host, port), 2000);
                    s.setSoTimeout(3000);
                    OutputStream out = s.getOutputStream();
                    InputStream in = s.getInputStream();

                    GetRequestResultRequestDto reqDto = new GetRequestResultRequestDto(requestId);
                    MessageEnvelope<GetRequestResultRequestDto> env = MessageEnvelope.createRequest(
                            MessageType.GET_REQUEST_RESULT,
                            "client-acc-test",
                            reqDto
                    );

                    FrameCodec.writeMessage(out, env);
                    byte[] resBytes = FrameCodec.readFrame(in);
                    MessageEnvelope<JsonNode> resEnv = MessageCodec.decode(resBytes);

                    if (resEnv.getType() == MessageType.GET_REQUEST_RESULT_RESPONSE) {
                        vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto wireDto =
                                MessageCodec.decodePayload(resEnv, vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto.class);
                        return new GetRequestResultResponseDto(
                                wireDto.getRequestId(),
                                wireDto.getState(),
                                wireDto.getCommandType(),
                                wireDto.getTicketId(),
                                wireDto.getReasonCode(),
                                wireDto.getAppliedLogIndex(),
                                false
                        );
                    } else {
                        return new GetRequestResultResponseDto(requestId, "UNKNOWN", null, null, null, null, false);
                    }
                } catch (Exception e) {
                    throw new CompletionException("Lỗi TCP tra cứu GET_REQUEST_RESULT: " + e.getMessage(), e);
                }
            });
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        serversToClose.clear();
        clientsToClose.clear();

        journal = new PersistentPendingJournal(tempJournalDir);
        seatsList = FXCollections.observableArrayList();
        for (int i = 1; i <= 10; i++) {
            seatsList.add(new SeatViewModel(String.format("G%02d", i), SeatViewModel.SeatStatus.AVAILABLE));
        }

        refreshService = new TrackingRefreshService(seatsList, journal);
        uiDispatchCount = new AtomicInteger(0);
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : clientsToClose) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
        for (FakeNodeServer s : serversToClose) {
            try {
                s.close();
            } catch (Exception ignored) {
            }
        }
        if (journal != null) {
            try {
                journal.close();
            } catch (Exception ignored) {
            }
        }
    }

    private FakeNodeServer createServer(String nodeId) throws IOException {
        FakeNodeServer server = new FakeNodeServer(nodeId);
        serversToClose.add(server);
        return server;
    }

    private BookingViewModel createViewModel(BookingClient client, int timeoutMs) {
        BookingViewModel.UiDispatcher dispatcher = action -> {
            uiDispatchCount.incrementAndGet();
            action.run(); // Thực thi đồng bộ trong test
        };

        BookingViewModel vm = new BookingViewModel(client, journal, refreshService, null, timeoutMs);
        vm.setUiDispatcher(dispatcher);
        vm.setSeatsList(seatsList);
        return vm;
    }

    private SeatViewModel.SeatStatus getSeatStatus(String seatId) {
        return seatsList.stream()
                .filter(s -> s.getSeatId().equals(seatId))
                .findFirst()
                .map(SeatViewModel::getStatus)
                .orElse(null);
    }

    // =========================================================================
    // 8 KỊCH BẢN CHẤP NHẬN UI THEO YÊU CẦU TV1-D20
    // =========================================================================

    @Test
    @DisplayName("TC_ACC_01: Node giả trả kết quả CONFIRMED cuối (AC02)")
    void TC_ACC_01_ServerConfirmed_BookingConfirmed() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer server = createServer("node-1");
        AcceptanceSocketBookingClient socketClient = new AcceptanceSocketBookingClient("127.0.0.1", server.getPort());
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 2000);

        String seatId = "G02";
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, getSeatStatus(seatId));

        CompletableFuture<BookTicketResponseDto> future = vm.book("SE-001", seatId, "Nguyễn Văn A");
        BookTicketResponseDto res = future.get(5, TimeUnit.SECONDS);

        assertNotNull(res);
        assertTrue(res.isConfirmed(), "Phản hồi phải có outcome=CONFIRMED");
        assertEquals(UiState.CONFIRMED, vm.getUiState(), "UI phải ở trạng thái CONFIRMED");
        assertEquals(SeatViewModel.SeatStatus.BOOKED, getSeatStatus(seatId), "Ghế G02 phải chuyển sang BOOKED");

        // Journal chuyển sang RESOLVED
        String reqId = vm.getCurrentRequestId();
        Optional<PendingEntry> entryOpt = journal.get(reqId);
        assertTrue(entryOpt.isPresent(), "Pending journal phải lưu requestId");
        assertEquals(PendingState.RESOLVED, entryOpt.get().getState(), "Journal phải chuyển trạng thái RESOLVED");
        assertEquals("CONFIRMED", entryOpt.get().getFinalOutcome());

        // SeatsRefreshService được gọi đúng 1 lần để đồng bộ sơ đồ ghế
        assertEquals(1, refreshService.refreshCount.get(), "SeatsRefreshService phải được kích hoạt refresh");
        assertTrue(vm.getMessage().contains("CONFIRMED") || vm.getMessage().contains("thành công"));
    }

    @Test
    @DisplayName("TC_ACC_02: Node giả trả lỗi NO_QUORUM có cấu trúc SPEC-03 (AC06)")
    void TC_ACC_02_NoQuorumResponse_NotSuccess() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer server = createServer("node-1");
        server.setForcedError("NO_QUORUM", "⚡ Lỗi cụm: Không đủ đa số (Quorum 3/5) để cam kết an toàn! (NO_QUORUM)");

        AcceptanceSocketBookingClient socketClient = new AcceptanceSocketBookingClient("127.0.0.1", server.getPort());
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 2000);

        String seatId = "G03";
        CompletableFuture<BookTicketResponseDto> future = vm.book("SE-001", seatId, "Trần Văn B");
        BookTicketResponseDto res = future.get(5, TimeUnit.SECONDS);

        assertNotNull(res);
        assertNotEquals(UiState.CONFIRMED, vm.getUiState(), "Tuyệt đối không được chuyển UiState sang CONFIRMED");
        assertEquals(UiState.ERROR, vm.getUiState(), "Lỗi có cấu trúc NO_QUORUM chuyển sang UiState.ERROR");
        assertEquals("NO_QUORUM", vm.getErrorCode());

        // Ghế KHÔNG chuyển sang BOOKED
        assertNotEquals(SeatViewModel.SeatStatus.BOOKED, getSeatStatus(seatId), "Ghế tuyệt đối không được chuyển sang BOOKED");
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, getSeatStatus(seatId), "Ghế giữ nguyên trạng thái AVAILABLE");

        // Banner cảnh báo cụm không đủ đa số
        assertTrue(vm.getMessage().contains("NO_QUORUM") || vm.getMessage().contains("Không đủ đa số"),
                "Banner phải cảnh báo cụm không đủ đa số 3/5");

        // KHÔNG sinh requestId mới
        String originalReqId = vm.getCurrentRequestId();
        assertNotNull(originalReqId);
        assertFalse(originalReqId.isBlank());

        // Pending journal được bảo toàn ở trạng thái SENT_UNRESOLVED để tra cứu/thử lại sau
        Optional<PendingEntry> opt = journal.get(originalReqId);
        assertTrue(opt.isPresent());
        assertEquals(PendingState.SENT_UNRESOLVED, opt.get().getState(), "NO_QUORUM giữ SENT_UNRESOLVED trong journal");
    }

    @Test
    @DisplayName("TC_ACC_03: Mất kết nối / timeout SAU KHI gửi byte ra socket -> UNKNOWN (AC10)")
    void TC_ACC_03_TimeoutPostSend_UnknownAndPending() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer server = createServer("node-1");
        server.setSimulateTimeoutOnBook(true, 3000); // Server giả lập giữ kết nối quá read timeout của client

        AcceptanceSocketBookingClient socketClient = new AcceptanceSocketBookingClient("127.0.0.1", server.getPort(), 1000, 1000);
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 2000);

        String seatId = "G04";
        CompletableFuture<BookTicketResponseDto> future = vm.book("SE-001", seatId, "Lê Văn C");

        BookTicketResponseDto res = future.get(5, TimeUnit.SECONDS);
        assertNotNull(res);
        assertEquals("UNKNOWN", res.getOutcome());

        // UI chuyển sang UNKNOWN
        assertEquals(UiState.UNKNOWN, vm.getUiState(), "Timeout sau khi gửi phải chuyển UiState sang UNKNOWN");
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, getSeatStatus(seatId), "Ghế phải chuyển sang UNKNOWN (viền cam)");

        // Pending journal bảo lưu SENT_UNRESOLVED
        String reqId = vm.getCurrentRequestId();
        Optional<PendingEntry> opt = journal.get(reqId);
        assertTrue(opt.isPresent());
        assertEquals(PendingState.SENT_UNRESOLVED, opt.get().getState(), "Journal phải bảo lưu SENT_UNRESOLVED");

        // Thông báo tuyệt đối không chứa các từ cấm
        String msg = vm.getMessage().toLowerCase();
        assertFalse(msg.contains("thành công"), "Thông báo UNKNOWN không được chứa 'thành công'");
        assertFalse(msg.contains("thất bại"), "Thông báo UNKNOWN không được chứa 'thất bại'");
        assertFalse(msg.contains("bị từ chối"), "Thông báo UNKNOWN không được chứa 'bị từ chối'");
        assertFalse(msg.contains("đã đặt"), "Thông báo UNKNOWN không được chứa 'đã đặt'");

        // Hiển thị requestId và có đường tra cứu/gửi lại
        assertNotNull(vm.getCurrentRequestId());
        assertEquals(reqId, vm.getCurrentRequestId());
    }

    @Test
    @DisplayName("TC_ACC_04: Mất kết nối TRƯỚC KHI gửi byte (cổng đóng) -> FAILED_NOT_SENT giữ AVAILABLE (AC10 / SPEC-06)")
    void TC_ACC_04_ConnectFailPreSend_KeepsAvailable() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        // Dùng cổng không có tiến trình nào lắng nghe (giả lập offline hoàn toàn)
        int unusedPort = 59999;
        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", unusedPort, 500, 1000, "client-pre-fail");
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 1000);

        String seatId = "G05";
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, getSeatStatus(seatId));

        CompletableFuture<BookTicketResponseDto> future = vm.book("SE-001", seatId, "Phạm Văn D");
        try {
            future.get(3, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }

        // UI chuyển DISCONNECTED
        assertEquals(UiState.DISCONNECTED, vm.getUiState());
        assertEquals(ClientErrorKind.FAILED_NOT_SENT.name(), vm.getErrorCode());

        // Theo SPEC-06 / D19: Lỗi kết nối trước gửi (FAILED_NOT_SENT) -> Ghế GIỮ NGUYÊN AVAILABLE!
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, getSeatStatus(seatId),
                "Ghế phải giữ nguyên AVAILABLE khi mất kết nối trước khi gửi");
        assertNotEquals(SeatViewModel.SeatStatus.UNKNOWN, getSeatStatus(seatId));
    }

    @Test
    @DisplayName("TC_ACC_05: Follower trả NOT_LEADER có leaderHint -> Client redirect cùng requestId (AC07 / SPEC-03)")
    void TC_ACC_05_NotLeader_RedirectsWithSameRequestId() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer follower = createServer("node-1");
        FakeNodeServer leader = createServer("node-2");

        follower.setNotLeader("node-2", 2L);
        leader.setLeader();

        Map<String, NodeEndpoint> map = new HashMap<>();
        map.put("node-1", new NodeEndpoint("node-1", "127.0.0.1", follower.getPort()));
        map.put("node-2", new NodeEndpoint("node-2", "127.0.0.1", leader.getPort()));
        ClusterEndpoints endpoints = new ClusterEndpoints(map, "node-1");

        RedirectPolicy policy = new RedirectPolicy(3, 2000, 5000);
        LeaderAwareBookingClient leaderAwareClient = new LeaderAwareBookingClient(
                endpoints,
                ep -> new SocketBookingClient(ep.host(), ep.port(), 2000, 3000, "client-redirect-test"),
                policy,
                null,
                "node-1"
        );
        clientsToClose.add(leaderAwareClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(leaderAwareClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 3000);

        String seatId = "G06";
        CompletableFuture<BookTicketResponseDto> future = vm.book("SE-001", seatId, "Hoàng Văn E");
        BookTicketResponseDto res = future.get(6, TimeUnit.SECONDS);

        assertNotNull(res);
        assertTrue(res.isConfirmed(), "Sau khi redirect sang node-2, kết quả cuối phải là CONFIRMED");
        assertEquals(UiState.CONFIRMED, vm.getUiState());
        assertEquals(SeatViewModel.SeatStatus.BOOKED, getSeatStatus(seatId));

        // Phân định tính toàn vẹn: Follower có 0 vé, Leader giả có đúng 1 vé
        assertEquals(0, follower.getActiveTicketCount(seatId), "Follower không được ghi vé");
        assertEquals(1, leader.getActiveTicketCount(seatId), "Leader giả phải ghi đúng 1 vé");

        // RequestId không bị sinh mới
        String originalReqId = vm.getCurrentRequestId();
        assertEquals(originalReqId, res.getRequestId(), "requestId phải bất biến suốt chu kỳ redirect");
    }

    @Test
    @DisplayName("TC_ACC_06: Ghế đã bán -> nhận REJECTED(SEAT_ALREADY_BOOKED) (AC03 / AC04)")
    void TC_ACC_06_SeatAlreadyBooked_Rejected() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer server = createServer("node-1");
        String seatId = "G07";
        server.setSeatStatus(seatId, "BOOKED", "ticket-existing-007");

        AcceptanceSocketBookingClient socketClient = new AcceptanceSocketBookingClient("127.0.0.1", server.getPort());
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 2000);

        CompletableFuture<BookTicketResponseDto> future = vm.book("SE-001", seatId, "Đặng Văn F");
        BookTicketResponseDto res = future.get(5, TimeUnit.SECONDS);

        assertNotNull(res);
        assertTrue(res.isRejected(), "Outcome phải là REJECTED");
        assertEquals("SEAT_ALREADY_BOOKED", res.getReasonCode(), "reasonCode phải là SEAT_ALREADY_BOOKED");

        assertEquals(UiState.REJECTED, vm.getUiState());
        assertEquals(SeatViewModel.SeatStatus.BOOKED, getSeatStatus(seatId), "Ghế phải chuyển sang BOOKED");

        // Journal chuyển sang RESOLVED
        String reqId = vm.getCurrentRequestId();
        Optional<PendingEntry> opt = journal.get(reqId);
        assertTrue(opt.isPresent());
        assertEquals(PendingState.RESOLVED, opt.get().getState());
        assertEquals("REJECTED", opt.get().getFinalOutcome());

        // SeatsRefreshService được gọi để cập nhật toàn bộ sơ đồ ghế
        assertEquals(1, refreshService.refreshCount.get(), "SeatsRefreshService phải được kích hoạt làm mới");
    }

    @Test
    @DisplayName("TC_ACC_07: Từ UNKNOWN, người dùng tra cứu kết quả (GET_REQUEST_RESULT) giải quyết theo requestId (AC10 / SPEC-03 §4.4)")
    void TC_ACC_07_UnknownThenLookup_ResolvesByRequestId() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer server = createServer("node-1");
        AcceptanceSocketBookingClient socketClient = new AcceptanceSocketBookingClient("127.0.0.1", server.getPort());
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 2000);

        String seatId = "G08";
        String reqIdSuccess = UUID.randomUUID().toString();

        // Giả lập trạng thái ban đầu của giao dịch trong journal và UI: UNKNOWN
        PendingEntry pendingEntry = new PendingEntry(
                reqIdSuccess,
                "BOOK_TICKET",
                "{\"requestId\":\"" + reqIdSuccess + "\",\"tripId\":\"SE-001\",\"seatId\":\"" + seatId + "\",\"passengerName\":\"Khách UNKNOWN\"}",
                PendingEntry.computePayloadHash("{\"requestId\":\"" + reqIdSuccess + "\",\"tripId\":\"SE-001\",\"seatId\":\"" + seatId + "\",\"passengerName\":\"Khách UNKNOWN\"}"),
                PendingState.SENT_UNRESOLVED,
                Instant.now(),
                null,
                1,
                "node-1",
                null,
                1
        );
        journal.recordPrepared(pendingEntry);
        journal.markSent(reqIdSuccess);

        // Kịch bản 7a: Node giả đã ghi nhận thành công từ trước -> lookup trả APPLIED_SUCCESS
        // Ta ghi trước booking vào FakeNodeServer để FakeNodeServer trả APPLIED_SUCCESS
        BookTicketRequestDto successReq = new BookTicketRequestDto(reqIdSuccess, "SE-001", seatId, "Khách UNKNOWN");
        socketClient.book(successReq).get(3, TimeUnit.SECONDS); // Server đã lưu storedBooking

        // Bây giờ thực hiện tra cứu qua BookingViewModel
        CompletableFuture<GetRequestResultResponseDto> lookupFuture = vm.getRequestResult(reqIdSuccess);
        GetRequestResultResponseDto lookupRes = lookupFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(lookupRes);
        assertEquals("APPLIED_SUCCESS", lookupRes.getState());
        assertEquals(UiState.CONFIRMED, vm.getUiState(), "Tra cứu thành công chuyển UiState.CONFIRMED");
        assertEquals(SeatViewModel.SeatStatus.BOOKED, getSeatStatus(seatId), "Ghế cập nhật sang BOOKED");
        assertEquals(PendingState.RESOLVED, journal.get(reqIdSuccess).get().getState(), "Journal chuyển RESOLVED");

        // Kịch bản 7b: Tra cứu yêu cầu không tồn tại trên Node -> trả UNKNOWN
        String reqIdNotFound = UUID.randomUUID().toString();
        CompletableFuture<GetRequestResultResponseDto> notFoundFuture = vm.getRequestResult(reqIdNotFound);
        GetRequestResultResponseDto notFoundRes = notFoundFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(notFoundRes);
        assertEquals("UNKNOWN", notFoundRes.getState());
        assertEquals(UiState.UNKNOWN, vm.getUiState(), "Chưa có kết quả thì giữ nguyên UiState.UNKNOWN");
        assertTrue(vm.getCheckCount() >= 2, "checkCount phải tăng sau mỗi lần tra cứu");
    }

    @Test
    @DisplayName("TC_ACC_08: Guard cụm thật 5 Node (cổng 9101-9105) bằng biến môi trường và kiểm tra cổng")
    void TC_ACC_08_RealCluster_GuardedByEnvironment() {
        String profile = System.getenv("CLUSTER_PROFILE");
        boolean isDevOrLan = "dev".equalsIgnoreCase(profile) || "lan".equalsIgnoreCase(profile);
        Assumptions.assumeTrue(isDevOrLan,
                "Cụm 5 Node thật chưa kích hoạt (CLUSTER_PROFILE!=dev|lan) -> SKIPPED (ghi nhận NOT_RUN / BLOCKED chờ G5)");

        boolean portsOpen = areClusterPortsOpen();
        Assumptions.assumeTrue(portsOpen,
                "Cụm 5 Node thật (ports 9101-9105) đang tắt -> SKIPPED (ghi nhận NOT_RUN / BLOCKED chờ G5)");

        fail("Nếu đến đây nghĩa là cụm thật đang chạy, nhưng theo SPEC không chạy hành vi phá hoại lên cụm thật.");
    }

    @Test
    @DisplayName("TC_ACC_09: Kiểm tra Dispatcher đồng bộ và chống double-click khi SUBMITTING")
    void TC_ACC_09_UiDispatcherAndAntiDoubleClick() throws Exception {
        // FAKE: quorum/Leader/commit do fixture giả lập, không phải Raft thật
        FakeNodeServer server = createServer("node-1");
        server.setDelayMs(200); // Tạo độ trễ để kiểm tra chống double-click

        AcceptanceSocketBookingClient socketClient = new AcceptanceSocketBookingClient("127.0.0.1", server.getPort());
        clientsToClose.add(socketClient);

        JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, journal);
        BookingViewModel vm = createViewModel(journalingClient, 2000);

        String seatId = "G09";

        // Bấm đặt lần 1
        CompletableFuture<BookTicketResponseDto> f1 = vm.book("SE-001", seatId, "Đặng Văn G");
        assertTrue(vm.isBusy() || vm.getUiState() == UiState.SUBMITTING, "UI phải ở trạng thái busy/SUBMITTING");

        // Bấm đặt lần 2 ngay lập tức khi đang SUBMITTING
        CompletableFuture<BookTicketResponseDto> f2 = vm.book("SE-001", seatId, "Đặng Văn G Lần 2");
        assertNotNull(f2);
        assertTrue(f2.isCompletedExceptionally(), "Bấm đúp khi đang SUBMITTING phải bị từ chối ngay lập tức");

        // Chờ hoàn tất lần 1
        BookTicketResponseDto res1 = f1.get(5, TimeUnit.SECONDS);
        assertNotNull(res1);
        assertTrue(res1.isConfirmed());

        // Kiểm tra chỉ duy nhất 1 request đến server
        assertEquals(1, server.getReceivedRequests().size(), "Chỉ duy nhất 1 request được gửi ra transport");

        // Kiểm tra mọi cập nhật giao diện đều đi qua UiDispatcher
        assertTrue(uiDispatchCount.get() > 0, "Mọi cập nhật property của ViewModel phải đi qua UiDispatcher");
    }

    private boolean areClusterPortsOpen() {
        for (int port = 9101; port <= 9105; port++) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 150);
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }
}
