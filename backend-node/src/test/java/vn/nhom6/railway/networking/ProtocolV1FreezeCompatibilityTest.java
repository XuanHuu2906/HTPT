package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Protocol v1 Freeze & Client-Node Wire Compatibility Tests (TV3-D28)")
public class ProtocolV1FreezeCompatibilityTest {

    private static JsonNode fixturesRoot;
    private final List<AutoCloseable> resources = new ArrayList<>();

    @BeforeAll
    static void loadProtocolV1Fixtures() throws IOException {
        Path fromRepoRoot = Path.of("shared/src/test/resources/fixtures/protocol-v1.json");
        Path fromBackendModule = Path.of("../shared/src/test/resources/fixtures/protocol-v1.json");
        Path fixturePath = Files.exists(fromRepoRoot) ? fromRepoRoot : fromBackendModule;
        assertTrue(Files.exists(fixturePath), "protocol-v1.json fixture file must exist at " + fixturePath.toAbsolutePath());
        fixturesRoot = MessageCodec.getMapper().readTree(Files.readString(fixturePath, StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception ignored) {
            }
        }
        resources.clear();
    }

    @Test
    @DisplayName("Frozen v1 metadata and DEV/LAN port mapping constants match SPEC-03 and SPEC-08")
    void testFrozenProtocolConstantsAndPortTable() {
        assertEquals(1, MessageDispatcher.FROZEN_PROTOCOL_VERSION);
        assertEquals(MessageEnvelope.CURRENT_PROTOCOL_VERSION, MessageDispatcher.FROZEN_PROTOCOL_VERSION);
        assertTrue(MessageDispatcher.isSupportedProtocolVersion(1));
        assertFalse(MessageDispatcher.isSupportedProtocolVersion(0));
        assertFalse(MessageDispatcher.isSupportedProtocolVersion(2));

        assertEquals(17, MessageDispatcher.FROZEN_V1_MESSAGE_TYPES.size());
        for (MessageType type : MessageType.values()) {
            assertTrue(MessageDispatcher.isFrozenV1MessageType(type), "Missing frozen type: " + type);
        }
        assertFalse(MessageDispatcher.isFrozenV1MessageType(null));

        // LAN standard ports
        assertEquals(9100, MessageDispatcher.STANDARD_CLIENT_PORT);
        assertEquals(9200, MessageDispatcher.STANDARD_PEER_PORT);
        assertEquals(MessageDispatcher.ChannelType.CLIENT, MessageDispatcher.resolveChannelForPort(9100));
        assertEquals(MessageDispatcher.ChannelType.PEER, MessageDispatcher.resolveChannelForPort(9200));

        // DEV published ports (node-1..node-5)
        for (int p = 9101; p <= 9105; p++) {
            assertTrue(MessageDispatcher.isStandardClientPort(p));
            assertEquals(MessageDispatcher.ChannelType.CLIENT, MessageDispatcher.resolveChannelForPort(p));
        }
        for (int p = 9201; p <= 9205; p++) {
            assertTrue(MessageDispatcher.isStandardPeerPort(p));
            assertEquals(MessageDispatcher.ChannelType.PEER, MessageDispatcher.resolveChannelForPort(p));
        }
    }

    @Test
    @DisplayName("Old v1 Client and Release Node parse and exchange all 17 MessageTypes over TCP framing")
    void testOldClientAndReleaseNodeExchangeAllProtocolV1Types() throws Exception {
        HandlerRegistry clientRegistry = new HandlerRegistry()
                .register(MessageType.GET_SEATS, req -> replyFromFixture(req, "getSeatsResponse"))
                .register(MessageType.BOOK_TICKET, req -> replyFromFixture(req, "bookTicketResponse"))
                .register(MessageType.CANCEL_TICKET, req -> replyFromFixture(req, "cancelTicketResponse"))
                .register(MessageType.GET_REQUEST_RESULT, req -> replyFromFixture(req, "getRequestResultResponse"))
                .register(MessageType.GET_TICKET_HISTORY, req -> replyFromFixture(req, "getTicketHistoryResponse"))
                .register(MessageType.NODE_STATUS, req -> replyFromFixture(req, "nodeStatusResponse"));

        HandlerRegistry peerRegistry = new HandlerRegistry()
                .register(MessageType.REQUEST_VOTE, req -> replyFromFixture(req, "requestVoteResponse"))
                .register(MessageType.APPEND_ENTRIES, req -> replyFromFixture(req, "appendEntriesResponse"));

        MessageDispatcher clientDispatcher = new MessageDispatcher(clientRegistry, MessageDispatcher.ChannelType.CLIENT, "node-3");
        MessageDispatcher peerDispatcher = new MessageDispatcher(peerRegistry, MessageDispatcher.ChannelType.PEER, "node-1");

        TcpServer clientServer = new TcpServer("127.0.0.1", 0, 4, 5000);
        clientServer.setMessageHandler(clientDispatcher);
        TcpServer peerServer = new TcpServer("127.0.0.1", 0, 4, 5000);
        peerServer.setMessageHandler(peerDispatcher);
        resources.add(clientServer);
        resources.add(peerServer);
        clientServer.start();
        peerServer.start();

        Set<MessageType> observedTypes = EnumSet.noneOf(MessageType.class);

        Map<String, Integer> requestFixtureToPort = Map.of(
                "getSeatsRequest", clientServer.getPort(),
                "bookTicketRequest", clientServer.getPort(),
                "cancelTicketRequest", clientServer.getPort(),
                "getRequestResultRequest", clientServer.getPort(),
                "getTicketHistoryRequest", clientServer.getPort(),
                "nodeStatusRequest", clientServer.getPort(),
                "requestVoteRequest", peerServer.getPort(),
                "appendEntriesReplicationRequest", peerServer.getPort(),
                "appendEntriesHeartbeatRequest", peerServer.getPort()
        );

        for (Map.Entry<String, Integer> entry : requestFixtureToPort.entrySet()) {
            JsonNode rawReq = fixturesRoot.get(entry.getKey());
            assertNotNull(rawReq, "Missing request fixture: " + entry.getKey());

            MessageEnvelope<JsonNode> reqEnv = MessageCodec.decode(rawReq.toString());
            observedTypes.add(reqEnv.getType());

            MessageEnvelope<JsonNode> respEnv = sendRawJsonOverTcp(
                    "127.0.0.1",
                    entry.getValue(),
                    rawReq.toString().getBytes(StandardCharsets.UTF_8),
                    3000
            );
            assertEquals(1, respEnv.getProtocolVersion());
            assertEquals(reqEnv.getMessageId(), respEnv.getCorrelationId(),
                    "correlationId must match request messageId for " + entry.getKey());
            assertTrue(respEnv.getType().isResponse());
            observedTypes.add(respEnv.getType());
        }

        // Also verify ERROR_RESPONSE golden fixture decodes cleanly on client parser
        JsonNode errorFixture = fixturesRoot.get("errorResponse");
        MessageEnvelope<ErrorResponseDto> decodedErr = MessageCodec.decode(errorFixture.toString(), ErrorResponseDto.class);
        decodedErr.validate();
        decodedErr.getPayload().validate();
        observedTypes.add(decodedErr.getType());

        assertEquals(EnumSet.allOf(MessageType.class), observedTypes,
                "All 17 MessageTypes must be verified across old-client <-> release-node parsing");
    }

    @Test
    @DisplayName("Unsupported protocolVersion is rejected safely on TCP wire and by MessageDispatcher without breaking v1")
    void testUnsupportedProtocolVersionRejectedNonBreaking() throws Exception {
        HandlerRegistry registry = new HandlerRegistry()
                .register(MessageType.GET_SEATS, req -> replyFromFixture(req, "getSeatsResponse"));
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");
        TcpServer server = new TcpServer("127.0.0.1", 0, 2, 5000);
        server.setMessageHandler(dispatcher);
        resources.add(server);
        server.start();

        String requestId = "542e32fb-b657-4478-9cea-52286822d1d2";
        String badMessageId = UUID.randomUUID().toString();
        ObjectNode badReq = (ObjectNode) fixturesRoot.get("bookTicketRequest").deepCopy();
        badReq.put("protocolVersion", 2);
        badReq.put("messageId", badMessageId);

        // 1. On TCP wire: MessageCodec/MessageEnvelope validation rejects protocolVersion=2 cleanly (EOF/IOException)
        assertThrows(IOException.class, () -> sendRawJsonOverTcp(
                "127.0.0.1",
                server.getPort(),
                badReq.toString().getBytes(StandardCharsets.UTF_8),
                3000
        ));

        // 2. At MessageDispatcher boundary (mutated envelope): returns INVALID_REQUEST ERROR_RESPONSE with correlationId
        MessageEnvelope<JsonNode> mutatedEnv = MessageCodec.decode(fixturesRoot.get("bookTicketRequest").toString());
        mutatedEnv.setMessageId(badMessageId);
        mutatedEnv.setProtocolVersion(2);
        MessageEnvelope<?> dispatchResp = dispatcher.dispatch(mutatedEnv);
        assertNotNull(dispatchResp);
        assertEquals(1, dispatchResp.getProtocolVersion());
        assertEquals(MessageType.ERROR_RESPONSE, dispatchResp.getType());
        assertEquals(badMessageId, dispatchResp.getCorrelationId());
        ErrorResponseDto errDto = (ErrorResponseDto) dispatchResp.getPayload();
        assertEquals("INVALID_REQUEST", errDto.getErrorCode());
        assertEquals(requestId, errDto.getRequestId());

        // 3. Subsequent valid v1 request still succeeds on same node
        JsonNode validGetSeats = fixturesRoot.get("getSeatsRequest");
        MessageEnvelope<JsonNode> okEnv = sendRawJsonOverTcp(
                "127.0.0.1",
                server.getPort(),
                validGetSeats.toString().getBytes(StandardCharsets.UTF_8),
                3000
        );
        assertEquals(MessageType.GET_SEATS_RESPONSE, okEnv.getType());
    }

    @Test
    @DisplayName("Retry semantics: preserve requestId across retries with fresh messageId and treat response loss as UNKNOWN")
    void testRetrySemanticsPreservesRequestIdAndGeneratesFreshMessageId() throws Exception {
        String stableRequestId = UUID.randomUUID().toString();
        AtomicInteger attempts = new AtomicInteger(0);
        List<String> observedMessageIds = new ArrayList<>();

        HandlerRegistry registry = new HandlerRegistry()
                .register(MessageType.BOOK_TICKET, req -> {
                    observedMessageIds.add(req.getMessageId());
                    return replyFromFixture(req, "bookTicketResponse");
                })
                .register(MessageType.GET_REQUEST_RESULT, req -> replyFromFixture(req, "getRequestResultResponse"));

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");
        // Drop first response to simulate response loss -> client must hit SocketTimeoutException (UNKNOWN)
        dispatcher.setResponseFilterHook((responder, sender, req, resp) ->
                req.getType() != MessageType.BOOK_TICKET || attempts.incrementAndGet() > 1);

        TcpServer server = new TcpServer("127.0.0.1", 0, 4, 5000);
        server.setMessageHandler(dispatcher);
        resources.add(server);
        server.start();

        RetryPolicy policy = new RetryPolicy(3, 10L, 50L, 2.0, 1000, 300);
        assertTrue(policy.canRetry(0));
        assertEquals(10L, policy.computeBackoffMs(0));
        assertEquals(20L, policy.computeBackoffMs(1));

        // Attempt 1: BOOK_TICKET with stableRequestId, response dropped -> SocketTimeoutException (UNKNOWN)
        ObjectNode req1 = (ObjectNode) fixturesRoot.get("bookTicketRequest").deepCopy();
        String msgId1 = UUID.randomUUID().toString();
        req1.put("messageId", msgId1);
        ((ObjectNode) req1.get("payload")).put("requestId", stableRequestId);

        assertThrows(SocketTimeoutException.class, () -> sendRawJsonOverTcp(
                "127.0.0.1", server.getPort(), req1.toString().getBytes(StandardCharsets.UTF_8), policy.readTimeoutMs()
        ));

        // Attempt 2 (retry): same requestId, fresh messageId -> succeeds
        ObjectNode req2 = (ObjectNode) fixturesRoot.get("bookTicketRequest").deepCopy();
        String msgId2 = UUID.randomUUID().toString();
        assertNotEquals(msgId1, msgId2);
        req2.put("messageId", msgId2);
        ((ObjectNode) req2.get("payload")).put("requestId", stableRequestId);

        MessageEnvelope<JsonNode> retryResp = sendRawJsonOverTcp(
                "127.0.0.1", server.getPort(), req2.toString().getBytes(StandardCharsets.UTF_8), policy.readTimeoutMs()
        );
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, retryResp.getType());
        assertEquals(msgId2, retryResp.getCorrelationId());
        assertEquals(List.of(msgId1, msgId2), observedMessageIds);
    }

    private static MessageEnvelope<JsonNode> replyFromFixture(MessageEnvelope<JsonNode> req, String responseFixtureKey) throws Exception {
        JsonNode fixture = fixturesRoot.get(responseFixtureKey);
        assertNotNull(fixture, "Missing response fixture: " + responseFixtureKey);
        MessageEnvelope<JsonNode> template = MessageCodec.decode(fixture.toString());
        return MessageEnvelope.createResponse(
                req.getMessageId(),
                template.getType(),
                template.getSenderId(),
                template.getPayload()
        );
    }

    private static MessageEnvelope<JsonNode> sendRawJsonOverTcp(String host, int port, byte[] jsonBytes, int readTimeoutMs)
            throws IOException {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(readTimeoutMs);
            socket.connect(new InetSocketAddress(host, port), 1500);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            FrameCodec.writeFrame(out, jsonBytes);
            byte[] responseBytes = FrameCodec.readFrame(in);
            return MessageCodec.decode(responseBytes);
        }
    }
}
