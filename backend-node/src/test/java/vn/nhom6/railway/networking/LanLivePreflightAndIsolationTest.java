package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV3-D25: Host LAN preflight, channel isolation (TC-TCP-04), and peer allowlist tests.
 *
 * <p>Verifies what can be proven on an available single host before the physical 5-laptop Gate G6:
 * <ul>
 *   <li>Standard port helper mapping in {@link MessageDispatcher}.</li>
 *   <li>Optional {@code allowedPeerIds} sanity check on {@link MessageDispatcher.ChannelType#PEER}.</li>
 *   <li>Real TCP socket isolation across separate Client and Raft Peer servers in {@link NodeMain}.</li>
 *   <li>Local host interface inspection and raw {@code PING/PONG} + {@code NODE_STATUS} probes.</li>
 * </ul>
 */
@DisplayName("LAN Connectivity Preflight & MessageDispatcher Isolation Tests (TV3-D25)")
public class LanLivePreflightAndIsolationTest {

    private static final Logger LOGGER = Logger.getLogger(LanLivePreflightAndIsolationTest.class.getName());
    private static final Set<String> FIVE_VOTERS = Set.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    @Test
    @DisplayName("TC-D25-01: Standard client (9100/9101..9105) and peer (9200/9201..9205) port checks")
    void testStandardClientAndPeerPortHelpers() {
        assertTrue(MessageDispatcher.isStandardClientPort(9100));
        for (int p = 9101; p <= 9105; p++) {
            assertTrue(MessageDispatcher.isStandardClientPort(p));
            assertFalse(MessageDispatcher.isStandardPeerPort(p));
        }

        assertTrue(MessageDispatcher.isStandardPeerPort(9200));
        for (int p = 9201; p <= 9205; p++) {
            assertTrue(MessageDispatcher.isStandardPeerPort(p));
            assertFalse(MessageDispatcher.isStandardClientPort(p));
        }

        assertFalse(MessageDispatcher.isStandardClientPort(8080));
        assertFalse(MessageDispatcher.isStandardPeerPort(5432));
    }

    @Test
    @DisplayName("TC-D25-02: PEER channel enforces allowedPeerIds when configured (SPEC-08 §3.1)")
    void testMessageDispatcherPeerAllowlistUnit() throws Exception {
        HandlerRegistry peerRegistry = new HandlerRegistry();
        FakeRaftRequestHandler fakeProcessor = new FakeRaftRequestHandler(1L);
        peerRegistry.register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(fakeProcessor, "node-1"));

        MessageDispatcher dispatcher = new MessageDispatcher(
                peerRegistry,
                MessageDispatcher.ChannelType.PEER,
                "node-1"
        );
        dispatcher.setAllowedPeerIds(FIVE_VOTERS);
        assertEquals(FIVE_VOTERS, dispatcher.getAllowedPeerIds());

        // 1. Valid remote voter -> accepted
        MessageEnvelope<JsonNode> validVote = toJsonEnvelope(MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE,
                "node-2",
                new RequestVoteRequestDto(1L, "node-2", 0L, 0L)
        ));
        MessageEnvelope<?> validResp = dispatcher.dispatch(validVote);
        assertNotNull(validResp);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, validResp.getType());

        // 2. Unconfigured rogue senderId -> rejected with INVALID_REQUEST
        MessageEnvelope<JsonNode> rogueVote = toJsonEnvelope(MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE,
                "rogue-node-99",
                new RequestVoteRequestDto(1L, "rogue-node-99", 0L, 0L)
        ));
        MessageEnvelope<?> rogueResp = dispatcher.dispatch(rogueVote);
        assertNotNull(rogueResp);
        assertEquals(MessageType.ERROR_RESPONSE, rogueResp.getType());
        ErrorResponseDto rogueErr = (ErrorResponseDto) rogueResp.getPayload();
        assertEquals("INVALID_REQUEST", rogueErr.getErrorCode());

        // 3. Self senderId spoofed on PEER channel -> rejected with INVALID_REQUEST
        MessageEnvelope<JsonNode> selfVote = toJsonEnvelope(MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE,
                "node-1",
                new RequestVoteRequestDto(1L, "node-1", 0L, 0L)
        ));
        MessageEnvelope<?> selfResp = dispatcher.dispatch(selfVote);
        assertNotNull(selfResp);
        assertEquals(MessageType.ERROR_RESPONSE, selfResp.getType());
        ErrorResponseDto selfErr = (ErrorResponseDto) selfResp.getPayload();
        assertEquals("INVALID_REQUEST", selfErr.getErrorCode());

        // 4. Setting null disables allowlist check (backwards compatible)
        dispatcher.setAllowedPeerIds(null);
        assertNull(dispatcher.getAllowedPeerIds());
        MessageEnvelope<?> relaxedResp = dispatcher.dispatch(toJsonEnvelope(MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE,
                "external-test-peer",
                new RequestVoteRequestDto(2L, "external-test-peer", 0L, 0L)
        )));
        assertNotNull(relaxedResp);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, relaxedResp.getType());
    }

    @Test
    @DisplayName("TC-D25-03: Real TCP socket channel isolation & peer allowlist on NodeMain (TC-TCP-04)")
    void testRealTcpChannelIsolationAndPeerAllowlist() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        NodeMain node = new NodeMain(config, new FakeBookingService("node-1"));
        resources.add(node);

        FakeRaftRequestHandler processor = new FakeRaftRequestHandler(3L);
        node.getRaftRegistry().register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(processor, "node-1"));
        node.getRaftDispatcher().setAllowedPeerIds(FIVE_VOTERS);
        node.start();

        int clientPort = node.getClientPort();
        int raftPort = node.getRaftPort();
        assertNotEquals(clientPort, raftPort);

        // 1. Client port rejects Raft REQUEST_VOTE (TC-TCP-04)
        try (TcpClient client = new TcpClient("127.0.0.1", clientPort, 1500, 3000)) {
            client.connect();

            // Verify PING -> PONG works on Client port
            byte[] pong = client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
            assertEquals("PONG", new String(pong, StandardCharsets.UTF_8));

            // Verify NODE_STATUS works on Client port
            MessageEnvelope<?> statusEnv = MessageEnvelope.createRequest(
                    MessageType.NODE_STATUS,
                    "monitor-preflight",
                    java.util.Map.of()
            );
            byte[] statusBytes = client.sendAndReceive(MessageCodec.encode(statusEnv));
            MessageEnvelope<NodeStatusDto> statusResp = MessageCodec.decode(statusBytes, NodeStatusDto.class);
            assertEquals(MessageType.NODE_STATUS_RESPONSE, statusResp.getType());
            assertEquals("node-1", statusResp.getPayload().getNodeId());

            // Inject REQUEST_VOTE onto Client port -> must reject with INVALID_REQUEST
            MessageEnvelope<?> leakedVote = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE,
                    "node-2",
                    new RequestVoteRequestDto(3L, "node-2", 0L, 0L)
            );
            byte[] errBytes = client.sendAndReceive(MessageCodec.encode(leakedVote));
            MessageEnvelope<ErrorResponseDto> errResp = MessageCodec.decode(errBytes, ErrorResponseDto.class);
            assertEquals(MessageType.ERROR_RESPONSE, errResp.getType());
            assertEquals("INVALID_REQUEST", errResp.getPayload().getErrorCode());
        }

        // 2. Peer port rejects BOOK_TICKET, rejects rogue peer, and accepts allowed voter node-2
        try (TcpClient peerClient = new TcpClient("127.0.0.1", raftPort, 1500, 3000)) {
            peerClient.connect();

            MessageEnvelope<?> leakedBooking = MessageEnvelope.createRequest(
                    MessageType.BOOK_TICKET,
                    "desktop-client-1",
                    new BookTicketRequestDto(
                            UUID.randomUUID().toString(), "SE-001", "G01", "Tester"
                    )
            );
            byte[] bookErrBytes = peerClient.sendAndReceive(MessageCodec.encode(leakedBooking));
            MessageEnvelope<ErrorResponseDto> bookErr = MessageCodec.decode(bookErrBytes, ErrorResponseDto.class);
            assertEquals(MessageType.ERROR_RESPONSE, bookErr.getType());
            assertEquals("INVALID_REQUEST", bookErr.getPayload().getErrorCode());

            MessageEnvelope<?> rogueVote = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE,
                    "rogue-laptop",
                    new RequestVoteRequestDto(3L, "rogue-laptop", 0L, 0L)
            );
            byte[] rogueBytes = peerClient.sendAndReceive(MessageCodec.encode(rogueVote));
            MessageEnvelope<ErrorResponseDto> rogueErr = MessageCodec.decode(rogueBytes, ErrorResponseDto.class);
            assertEquals(MessageType.ERROR_RESPONSE, rogueErr.getType());
            assertEquals("INVALID_REQUEST", rogueErr.getPayload().getErrorCode());

            MessageEnvelope<?> validVote = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE,
                    "node-2",
                    new RequestVoteRequestDto(3L, "node-2", 0L, 0L)
            );
            byte[] voteBytes = peerClient.sendAndReceive(MessageCodec.encode(validVote));
            MessageEnvelope<RequestVoteResponseDto> voteResp = MessageCodec.decode(voteBytes, RequestVoteResponseDto.class);
            assertEquals(MessageType.REQUEST_VOTE_RESPONSE, voteResp.getType());
            assertTrue(voteResp.getPayload().isVoteGranted());
        }
    }

    @Test
    @DisplayName("TC-D25-04: Available host interface and standard port bindability preflight")
    void testHostInterfacesAndPortBindabilityPreflight() throws Exception {
        List<String> ipv4Addresses = new ArrayList<>();
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            if (!iface.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addrs = iface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress addr = addrs.nextElement();
                if (addr instanceof Inet4Address) {
                    ipv4Addresses.add(iface.getName() + "=" + addr.getHostAddress());
                }
            }
        }
        assertFalse(ipv4Addresses.isEmpty(), "Host must expose at least one active IPv4 address (e.g. lo=127.0.0.1)");

        boolean port9100Free = canBindPort(MessageDispatcher.STANDARD_CLIENT_PORT);
        boolean port9200Free = canBindPort(MessageDispatcher.STANDARD_PEER_PORT);
        LOGGER.info(() -> "HOST_PREFLIGHT ipv4=" + ipv4Addresses
                + " port9100Bindable=" + port9100Free
                + " port9200Bindable=" + port9200Free
                + " PHYSICAL_5_LAPTOP_G6=NOT_RUN");
    }

    private static boolean canBindPort(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            socket.setReuseAddress(true);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static MessageEnvelope<JsonNode> toJsonEnvelope(MessageEnvelope<?> envelope) throws Exception {
        return MessageCodec.decode(MessageCodec.encode(envelope), JsonNode.class);
    }
}
