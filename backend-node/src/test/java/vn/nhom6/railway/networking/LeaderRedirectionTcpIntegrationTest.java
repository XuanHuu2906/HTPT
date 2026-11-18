package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.Role;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TCP Socket Integration Tests for Leader Redirection & Forwarding (TASK TV3-D13)")
public class LeaderRedirectionTcpIntegrationTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        resources.clear();
    }

    @AfterEach
    public void tearDown() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {
            }
        }
        resources.clear();
    }

    @Test
    @DisplayName("Pass Criteria 1: Gửi BOOK tới Follower nhận hint NOT_LEADER đúng 1 lần với requestId nguyên vẹn")
    public void testSendBookToFollowerReceivesNotLeaderHint() throws Exception {
        // Start Follower Node (node-2) with leaderHint pointing to node-1
        NodeConfig config2 = new NodeConfig("node-2", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService fakeService2 = new FakeBookingService("node-2");
        fakeService2.setLeader(false, "node-1");

        NodeMain followerNode = new NodeMain(config2, fakeService2);
        resources.add(followerNode);
        followerNode.start();

        // Connect client to Follower
        TcpClient client = new TcpClient("127.0.0.1", followerNode.getClientPort(), 3000, 3000);
        resources.add(client);
        client.connect();
        assertTrue(client.isConnected());

        // Send BOOK_TICKET request to Follower
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G04", "Passenger Follower Test");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-desktop-tv1", bookDto
        );

        MessageEnvelope<ErrorResponseDto> response = client.sendAndReceive(request, ErrorResponseDto.class);

        // Verification
        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());
        assertEquals("node-2", response.getSenderId());

        ErrorResponseDto errorDto = response.getPayload();
        errorDto.validate();
        assertEquals("NOT_LEADER", errorDto.getErrorCode());
        assertEquals("node-1", errorDto.getLeaderHint(), "leaderHint must point to leader node-1");
        assertEquals(reqId, errorDto.getRequestId(), "requestId must be preserved without modification");
    }

    @Test
    @DisplayName("End-to-End Client Redirection: Follower trả hint -> Client redirect sang Leader thành công")
    public void testEndToEndClientRedirectToLeader() throws Exception {
        // 1. Leader Node (node-1)
        NodeConfig config1 = new NodeConfig("node-1", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService leaderService = new FakeBookingService("node-1");
        leaderService.setLeader(true, "node-1");
        NodeMain leaderNode = new NodeMain(config1, leaderService);
        resources.add(leaderNode);
        leaderNode.start();

        // 2. Follower Node (node-2)
        NodeConfig config2 = new NodeConfig("node-2", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService followerService = new FakeBookingService("node-2");
        followerService.setLeader(false, "node-1");
        NodeMain followerNode = new NodeMain(config2, followerService);
        resources.add(followerNode);
        followerNode.start();

        // 3. Client connects to Follower first
        TcpClient followerClient = new TcpClient("127.0.0.1", followerNode.getClientPort(), 3000, 3000);
        resources.add(followerClient);
        followerClient.connect();

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G07", "Redirected Passenger");
        MessageEnvelope<BookTicketRequestDto> reqToFollower = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-desktop", bookDto
        );

        MessageEnvelope<ErrorResponseDto> notLeaderResp = followerClient.sendAndReceive(reqToFollower, ErrorResponseDto.class);
        assertEquals("NOT_LEADER", notLeaderResp.getPayload().getErrorCode());
        assertEquals("node-1", notLeaderResp.getPayload().getLeaderHint());
        assertEquals(reqId, notLeaderResp.getPayload().getRequestId());

        // 4. Client follows hint and connects to Leader Node (node-1) with SAME requestId
        TcpClient leaderClient = new TcpClient("127.0.0.1", leaderNode.getClientPort(), 3000, 3000);
        resources.add(leaderClient);
        leaderClient.connect();

        MessageEnvelope<BookTicketRequestDto> reqToLeader = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-desktop", bookDto
        );
        MessageEnvelope<BookTicketResponseDto> confirmResp = leaderClient.sendAndReceive(reqToLeader, BookTicketResponseDto.class);

        assertNotNull(confirmResp);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, confirmResp.getType());
        BookTicketResponseDto bookResult = confirmResp.getPayload();
        bookResult.validate();
        assertTrue(bookResult.isConfirmed());
        assertEquals(reqId, bookResult.getRequestId(), "Confirmed booking must match original requestId");
        assertEquals("G07", bookResult.getSeatId());
        assertNotNull(bookResult.getTicketId());
    }

    @Test
    @DisplayName("Pass Criteria 2: Gửi BOOK tới Follower ở chế độ Forward nhận forwarded response đúng 1 lần")
    public void testFollowerTransparentForwardingToLeader() throws Exception {
        // 1. Leader Node (node-1)
        NodeConfig config1 = new NodeConfig("node-1", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService leaderService = new FakeBookingService("node-1");
        leaderService.setLeader(true, "node-1");
        NodeMain leaderNode = new NodeMain(config1, leaderService);
        resources.add(leaderNode);
        leaderNode.start();

        // 2. Follower Node (node-2) with FORWARD mode configured
        AtomicInteger forwardCounter = new AtomicInteger(0);
        int leaderClientPort = leaderNode.getClientPort();

        LeaderRouter.RequestForwarder tcpForwarder = (targetNodeId, requestEnvelope) -> {
            forwardCounter.incrementAndGet();
            assertEquals("node-1", targetNodeId);
            // Forward via TCP to leader
            try (TcpClient peerClient = new TcpClient("127.0.0.1", leaderClientPort, 3000, 3000)) {
                peerClient.connect();
                // Send original request with same payload and requestId
                return peerClient.sendAndReceive(requestEnvelope, BookTicketResponseDto.class);
            }
        };

        LeaderRouter followerRouter = new LeaderRouter(
                "node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 6L, "node-1"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
                LeaderRouter.Mode.FORWARD,
                tcpForwarder
        );

        // Start server for node-2 using custom BookTicketHandler with forwarding router
        HandlerRegistry registry2 = new HandlerRegistry();
        FakeBookingService followerService = new FakeBookingService("node-2");
        followerService.setLeader(false, "node-1");
        registry2.register(MessageType.BOOK_TICKET, new BookTicketHandler(followerService, "node-2", followerRouter));

        MessageDispatcher dispatcher2 = new MessageDispatcher(registry2, MessageDispatcher.ChannelType.CLIENT, "node-2");
        TcpServer followerServer = new TcpServer("127.0.0.1", 0, 4, 3000);
        followerServer.setMessageHandler(dispatcher2);
        followerServer.start();
        resources.add(followerServer);

        // 3. Client connects to Follower (node-2)
        TcpClient client = new TcpClient("127.0.0.1", followerServer.getPort(), 3000, 3000);
        resources.add(client);
        client.connect();

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G02", "Forwarded Passenger");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> response = client.sendAndReceive(request, BookTicketResponseDto.class);

        // Assertions: Client gets confirmed booking directly through follower
        assertNotNull(response);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, response.getType());
        BookTicketResponseDto confirm = response.getPayload();
        confirm.validate();
        assertTrue(confirm.isConfirmed());
        assertEquals(reqId, confirm.getRequestId(), "Forwarded response must preserve requestId");
        assertEquals("G02", confirm.getSeatId());
        assertNotNull(confirm.getTicketId());

        // Forward was performed EXACTLY 1 time
        assertEquals(1, forwardCounter.get(), "Request must be forwarded exactly 1 time");
    }

    @Test
    @DisplayName("Forwarding Loop Prevention over TCP: Ngăn chặn lặp chuyển tiếp khi nhận từ peer")
    public void testForwardingLoopHaltedWhenRequestFromPeer() throws Exception {
        AtomicInteger forwardCounter = new AtomicInteger(0);
        LeaderRouter.RequestForwarder forwarder = (target, request) -> {
            forwardCounter.incrementAndGet();
            return null;
        };

        LeaderRouter followerRouter = new LeaderRouter(
                "node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 1L, "node-3"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
                LeaderRouter.Mode.FORWARD,
                forwarder
        );

        HandlerRegistry registry = new HandlerRegistry();
        FakeBookingService service = new FakeBookingService("node-2");
        service.setLeader(false, "node-3");
        registry.register(MessageType.BOOK_TICKET, new BookTicketHandler(service, "node-2", followerRouter));

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-2");
        TcpServer server = new TcpServer("127.0.0.1", 0, 4, 3000);
        server.setMessageHandler(dispatcher);
        server.start();
        resources.add(server);

        TcpClient client = new TcpClient("127.0.0.1", server.getPort(), 3000, 3000);
        resources.add(client);
        client.connect();

        // Request with senderId = "node-1" (another peer node in the cluster)
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G05", "Loop Passenger");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "node-1", bookDto
        );

        MessageEnvelope<ErrorResponseDto> response = client.sendAndReceive(request, ErrorResponseDto.class);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals("NOT_LEADER", response.getPayload().getErrorCode());
        assertEquals(reqId, response.getPayload().getRequestId());

        // Verified: Forwarding loop prevented, forward was NEVER executed
        assertEquals(0, forwardCounter.get(), "Must not forward request originating from a peer voter");
    }

    @Test
    @DisplayName("TV3-D17: MessageDispatcher-level redirect guarantees Follower NEVER writes to DB or confirms")
    public void testDispatcherLevelRedirectGuaranteesZeroDatabaseWrites() throws Exception {
        AtomicInteger bookingServiceCallCount = new AtomicInteger(0);

        // Booking service with call tracking
        FakeBookingService service = new FakeBookingService("node-2") {
            @Override
            public vn.nhom6.railway.booking.ApplyResult bookTicket(String requestId, String tripId, String seatId, String passengerName) throws Exception {
                bookingServiceCallCount.incrementAndGet();
                fail("BookingService should NEVER be called on a Follower!");
                return super.bookTicket(requestId, tripId, seatId, passengerName);
            }
        };
        service.setLeader(false, "node-1");

        NodeConfig config = new NodeConfig("node-2", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        NodeMain followerNode = new NodeMain(config, service);
        resources.add(followerNode);
        followerNode.start();

        TcpClient client = new TcpClient("127.0.0.1", followerNode.getClientPort(), 3000, 3000);
        resources.add(client);
        client.connect();

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G08", "Passenger Non-Leader");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<ErrorResponseDto> response = client.sendAndReceive(request, ErrorResponseDto.class);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        ErrorResponseDto err = response.getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-1", err.getLeaderHint());
        assertEquals(reqId, err.getRequestId(), "requestId must be strictly preserved");

        // Verify Follower safety invariant: DB write / booking service counter is exactly 0
        assertEquals(0, bookingServiceCallCount.get(), "Follower must NEVER touch booking storage or self-confirm");
    }

    @Test
    @DisplayName("TV3-D17: Fallback peer candidate rotation when leaderHint is null or stale over TCP")
    public void testFollowerFallbackCandidatesWhenHintNullOrStaleOverTcp() throws Exception {
        // Node 2 has null leaderHint
        FakeBookingService service = new FakeBookingService("node-2");
        service.setLeader(false, null);

        NodeConfig config = new NodeConfig("node-2", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        NodeMain followerNode = new NodeMain(config, service);
        resources.add(followerNode);
        followerNode.start();

        TcpClient client = new TcpClient("127.0.0.1", followerNode.getClientPort(), 3000, 3000);
        resources.add(client);
        client.connect();

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G09", "Passenger Fallback");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<ErrorResponseDto> response = client.sendAndReceive(request, ErrorResponseDto.class);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        ErrorResponseDto err = response.getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertNotNull(err.getLeaderHint(), "Fallback hint must not be null");
        assertNotEquals("node-2", err.getLeaderHint(), "Self-hint must NEVER be returned");
        assertTrue(List.of("node-1", "node-3", "node-4", "node-5").contains(err.getLeaderHint()),
                "Fallback must be one of the cluster peers");
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("TV3-D18: 6 concurrent independent booking requests over TCP succeed without cross-talk")
    public void testSixConcurrentIndependentBookingRequestsOverTcp() throws Exception {
        NodeConfig config1 = new NodeConfig("node-1", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 10, 3000);
        FakeBookingService leaderService = new FakeBookingService("node-1");
        leaderService.setLeader(true, "node-1");
        NodeMain leaderNode = new NodeMain(config1, leaderService);
        resources.add(leaderNode);
        leaderNode.start();

        int n = 6;
        ExecutorService executor = Executors.newFixedThreadPool(n);
        resources.add(executor::shutdownNow);

        List<Future<MessageEnvelope<BookTicketResponseDto>>> futures = new ArrayList<>();
        List<MessageEnvelope<BookTicketRequestDto>> requests = new ArrayList<>();

        for (int i = 1; i <= n; i++) {
            final int index = i;
            String seatId = String.format("G%02d", index);
            String reqId = UUID.randomUUID().toString();
            BookTicketRequestDto dto = new BookTicketRequestDto(reqId, "SE-001", seatId, "Passenger " + index);
            MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(
                    MessageType.BOOK_TICKET, "client-" + index, dto
            );
            requests.add(req);

            futures.add(executor.submit(() -> {
                try {
                    TcpClient client = new TcpClient("127.0.0.1", leaderNode.getClientPort(), 3000, 3000);
                    client.connect();
                    try {
                        return client.sendAndReceive(req, BookTicketResponseDto.class);
                    } finally {
                        client.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        List<String> ticketIds = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            MessageEnvelope<BookTicketResponseDto> resp = futures.get(i).get(10, TimeUnit.SECONDS);
            MessageEnvelope<BookTicketRequestDto> req = requests.get(i);
            assertNotNull(resp);
            assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());
            assertEquals(req.getMessageId(), resp.getCorrelationId(), "Correlation ID must strictly match request transport messageId");

            BookTicketResponseDto payload = resp.getPayload();
            payload.validate();
            assertEquals("CONFIRMED", payload.getOutcome());
            assertEquals(req.getPayload().getRequestId(), payload.getRequestId(), "Business requestId must be strictly preserved");
            ticketIds.add(payload.getTicketId());
        }

        assertEquals(6, ticketIds.stream().distinct().count(), "All 6 concurrent requests must yield distinct tickets");
    }

    @Test
    @DisplayName("TV3-D18: Follower redirect -> Leader retry with new messageId preserves requestId and idempotency over TCP")
    public void testFollowerRedirectThenLeaderRetryPreservesRequestIdAndIdempotencyOverTcp() throws Exception {
        // 1. Leader Node
        NodeConfig config1 = new NodeConfig("node-1", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService leaderService = new FakeBookingService("node-1");
        leaderService.setLeader(true, "node-1");
        NodeMain leaderNode = new NodeMain(config1, leaderService);
        resources.add(leaderNode);
        leaderNode.start();

        // 2. Follower Node
        NodeConfig config2 = new NodeConfig("node-2", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService followerService = new FakeBookingService("node-2");
        followerService.setLeader(false, "node-1");
        NodeMain followerNode = new NodeMain(config2, followerService);
        resources.add(followerNode);
        followerNode.start();

        String businessRequestId = UUID.randomUUID().toString();
        BookTicketRequestDto bookPayload = new BookTicketRequestDto(businessRequestId, "SE-001", "G07", "Passenger Idempotent TCP");

        // Step 1: Send request to Follower with messageId M1
        MessageEnvelope<BookTicketRequestDto> req1 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookPayload
        );
        TcpClient followerClient = new TcpClient("127.0.0.1", followerNode.getClientPort(), 3000, 3000);
        resources.add(followerClient);
        followerClient.connect();

        MessageEnvelope<ErrorResponseDto> followerResp = followerClient.sendAndReceive(req1, ErrorResponseDto.class);
        assertNotNull(followerResp);
        assertEquals(MessageType.ERROR_RESPONSE, followerResp.getType());
        assertEquals(req1.getMessageId(), followerResp.getCorrelationId(), "Follower correlationId must match M1");
        ErrorResponseDto followerErr = followerResp.getPayload();
        followerErr.validate();
        assertEquals("NOT_LEADER", followerErr.getErrorCode());
        assertEquals("node-1", followerErr.getLeaderHint());
        assertEquals(businessRequestId, followerErr.getRequestId());

        // Step 2: Client resolves hint and retries on Leader with SAME businessRequestId and NEW messageId M2
        MessageEnvelope<BookTicketRequestDto> req2 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookPayload
        );
        assertNotEquals(req1.getMessageId(), req2.getMessageId(), "Retry must have fresh transport messageId");

        TcpClient leaderClient = new TcpClient("127.0.0.1", leaderNode.getClientPort(), 3000, 3000);
        resources.add(leaderClient);
        leaderClient.connect();

        MessageEnvelope<BookTicketResponseDto> leaderResp1 = leaderClient.sendAndReceive(req2, BookTicketResponseDto.class);
        assertNotNull(leaderResp1);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, leaderResp1.getType());
        assertEquals(req2.getMessageId(), leaderResp1.getCorrelationId(), "Leader response must correlate to M2, NOT M1");
        BookTicketResponseDto book1 = leaderResp1.getPayload();
        book1.validate();
        assertEquals("CONFIRMED", book1.getOutcome());
        assertEquals(businessRequestId, book1.getRequestId());
        String initialTicketId = book1.getTicketId();
        assertNotNull(initialTicketId);

        // Step 3: Client retries AGAIN to Leader (e.g. simulated network reconnect) with SAME businessRequestId and NEW messageId M3
        MessageEnvelope<BookTicketRequestDto> req3 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookPayload
        );
        assertNotEquals(req2.getMessageId(), req3.getMessageId());

        MessageEnvelope<BookTicketResponseDto> leaderResp2 = leaderClient.sendAndReceive(req3, BookTicketResponseDto.class);
        assertNotNull(leaderResp2);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, leaderResp2.getType());
        assertEquals(req3.getMessageId(), leaderResp2.getCorrelationId(), "Retry response must correlate to M3");
        BookTicketResponseDto book2 = leaderResp2.getPayload();
        book2.validate();
        assertEquals("CONFIRMED", book2.getOutcome());
        assertEquals(businessRequestId, book2.getRequestId());
        assertEquals(initialTicketId, book2.getTicketId(), "Must return identical ticketId; no double-booking occurred");
    }
}
