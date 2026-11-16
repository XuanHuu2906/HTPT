package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TCP NodeStatus live integration tests (TV3-D15)")
public class NodeStatusTcpIntegrationTest {

    private final List<AutoCloseable> resources = new ArrayList<>();
    private NodeMain node;
    private TcpClient client;

    @BeforeEach
    public void setUp() throws IOException {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        node = new NodeMain(config, new FakeBookingService(config.nodeId()));
        resources.add(node);
        node.start();

        client = new TcpClient("127.0.0.1", node.getClientPort(), 3000, 5000);
        resources.add(client);
        client.connect();
        assertTrue(client.isConnected(), "Client must be connected to NodeMain client port");
    }

    @AfterEach
    public void tearDown() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    @Test
    @DisplayName("Should query NODE_STATUS over real TCP socket and decode NodeStatusDto")
    public void testNodeStatusOverTcp() throws Exception {
        MessageEnvelope<?> statusReq = MessageEnvelope.createRequest(
                MessageType.NODE_STATUS, "monitor-agent-1", Map.of()
        );

        MessageEnvelope<NodeStatusDto> resp = client.sendAndReceive(statusReq, NodeStatusDto.class);
        assertNotNull(resp);
        assertEquals(MessageType.NODE_STATUS_RESPONSE, resp.getType());
        assertEquals("node-1", resp.getSenderId());
        assertEquals(statusReq.getMessageId(), resp.getCorrelationId());

        NodeStatusDto payload = resp.getPayload();
        assertNotNull(payload);
        payload.validate();

        assertEquals("node-1", payload.getNodeId());
        assertNotNull(payload.getObservedAt());
        assertNotNull(payload.getPeerConnectivity());
        assertEquals(4, payload.getPeerConnectivity().size());
        assertFalse(payload.getPeerConnectivity().containsKey("node-1"));
        assertTrue(payload.getPeerConnectivity().containsKey("node-2"));
        assertTrue(payload.getPeerConnectivity().containsKey("node-3"));
        assertTrue(payload.getPeerConnectivity().containsKey("node-4"));
        assertTrue(payload.getPeerConnectivity().containsKey("node-5"));
    }

    @Test
    @DisplayName("Should handle repeated sequential NODE_STATUS requests over same TCP connection (Monitor polling simulation)")
    public void testRepeatedMonitorPollingOverTcp() throws Exception {
        for (int i = 0; i < 5; i++) {
            MessageEnvelope<?> req = MessageEnvelope.createRequest(
                    MessageType.NODE_STATUS, "monitor-poll-" + i, Map.of()
            );
            MessageEnvelope<NodeStatusDto> resp = client.sendAndReceive(req, NodeStatusDto.class);
            assertNotNull(resp);
            assertEquals(MessageType.NODE_STATUS_RESPONSE, resp.getType());
            assertEquals(req.getMessageId(), resp.getCorrelationId());

            NodeStatusDto payload = resp.getPayload();
            assertNotNull(payload);
            payload.validate();
            assertEquals("node-1", payload.getNodeId());
        }
    }
}
