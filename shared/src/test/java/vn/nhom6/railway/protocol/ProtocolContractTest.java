package vn.nhom6.railway.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolContractTest {

    private static JsonNode fixturesRoot;

    @BeforeAll
    static void loadFixtures() throws IOException {
        try (InputStream is = ProtocolContractTest.class.getClassLoader().getResourceAsStream("fixtures/protocol-v1.json")) {
            assertNotNull(is, "fixtures/protocol-v1.json must be present in classpath");
            fixturesRoot = MessageCodec.getMapper().readTree(is);
        }
    }

    @Test
    @DisplayName("Should decode all Golden Fixtures into strongly-typed envelopes and validate")
    void testDecodeGoldenFixtures() throws Exception {
        // 1. BOOK_TICKET request
        JsonNode bookNode = fixturesRoot.get("bookTicketRequest");
        assertNotNull(bookNode);
        MessageEnvelope<BookTicketRequestDto> bookEnv = MessageCodec.decode(
                bookNode.toString(), BookTicketRequestDto.class
        );
        bookEnv.validate();
        bookEnv.getPayload().validate();
        assertEquals(1, bookEnv.getProtocolVersion());
        assertEquals("b4c410df-2b27-45dd-81ac-b7f9c0ec21e4", bookEnv.getMessageId());
        assertNull(bookEnv.getCorrelationId());
        assertEquals(MessageType.BOOK_TICKET, bookEnv.getType());
        assertEquals("client-7", bookEnv.getSenderId());
        assertEquals("542e32fb-b657-4478-9cea-52286822d1d2", bookEnv.getPayload().getRequestId());
        assertEquals("SE-001", bookEnv.getPayload().getTripId());
        assertEquals("G05", bookEnv.getPayload().getSeatId());
        assertEquals("Khach demo", bookEnv.getPayload().getPassengerName());

        // 2. GET_SEATS request
        JsonNode getSeatsNode = fixturesRoot.get("getSeatsRequest");
        assertNotNull(getSeatsNode);
        MessageEnvelope<GetSeatsRequestDto> getSeatsEnv = MessageCodec.decode(
                getSeatsNode.toString(), GetSeatsRequestDto.class
        );
        getSeatsEnv.validate();
        getSeatsEnv.getPayload().validate();
        assertEquals(MessageType.GET_SEATS, getSeatsEnv.getType());
        assertEquals("SE-001", getSeatsEnv.getPayload().getTripId());

        // 3. REQUEST_VOTE request
        JsonNode voteNode = fixturesRoot.get("requestVoteRequest");
        assertNotNull(voteNode);
        MessageEnvelope<RequestVoteRequestDto> voteEnv = MessageCodec.decode(
                voteNode.toString(), RequestVoteRequestDto.class
        );
        voteEnv.validate();
        voteEnv.getPayload().validate();
        assertEquals(MessageType.REQUEST_VOTE, voteEnv.getType());
        assertEquals(8, voteEnv.getPayload().getTerm());
        assertEquals("node-3", voteEnv.getPayload().getCandidateId());
        assertEquals(42, voteEnv.getPayload().getLastLogIndex());
        assertEquals(7, voteEnv.getPayload().getLastLogTerm());

        // 4. APPEND_ENTRIES replication request
        JsonNode appendNode = fixturesRoot.get("appendEntriesReplicationRequest");
        assertNotNull(appendNode);
        MessageEnvelope<AppendEntriesRequestDto> appendEnv = MessageCodec.decode(
                appendNode.toString(), AppendEntriesRequestDto.class
        );
        appendEnv.validate();
        appendEnv.getPayload().validate();
        assertEquals(MessageType.APPEND_ENTRIES, appendEnv.getType());
        assertFalse(appendEnv.getPayload().isHeartbeat());
        assertEquals(1, appendEnv.getPayload().getEntries().size());
        assertEquals("BOOK_TICKET", appendEnv.getPayload().getEntries().get(0).getCommandType());

        // 5. APPEND_ENTRIES heartbeat request
        JsonNode hbNode = fixturesRoot.get("appendEntriesHeartbeatRequest");
        assertNotNull(hbNode);
        MessageEnvelope<AppendEntriesRequestDto> hbEnv = MessageCodec.decode(
                hbNode.toString(), AppendEntriesRequestDto.class
        );
        hbEnv.validate();
        hbEnv.getPayload().validate();
        assertTrue(hbEnv.getPayload().isHeartbeat());
        assertEquals(0, hbEnv.getPayload().getEntries().size());
    }

    @Test
    @DisplayName("Should maintain 100% round-trip equivalence against golden fixtures")
    void testRoundTripEquivalence() throws Exception {
        // BOOK_TICKET round trip
        JsonNode originalBookNode = fixturesRoot.get("bookTicketRequest");
        MessageEnvelope<BookTicketRequestDto> bookEnv = MessageCodec.decode(
                originalBookNode.toString(), BookTicketRequestDto.class
        );
        byte[] encodedBytes = MessageCodec.encode(bookEnv);
        JsonNode reDecodedNode = MessageCodec.getMapper().readTree(encodedBytes);

        // Verify JSON tree structure equality
        assertEquals(originalBookNode, reDecodedNode);
        assertTrue(new String(encodedBytes, StandardCharsets.UTF_8).contains("\"correlationId\":null"));

        // GET_SEATS round trip
        JsonNode originalGetSeatsNode = fixturesRoot.get("getSeatsRequest");
        MessageEnvelope<GetSeatsRequestDto> getSeatsEnv = MessageCodec.decode(
                originalGetSeatsNode.toString(), GetSeatsRequestDto.class
        );
        byte[] getSeatsBytes = MessageCodec.encode(getSeatsEnv);
        assertEquals(originalGetSeatsNode, MessageCodec.getMapper().readTree(getSeatsBytes));

        // REQUEST_VOTE round trip
        JsonNode originalVoteNode = fixturesRoot.get("requestVoteRequest");
        MessageEnvelope<RequestVoteRequestDto> voteEnv = MessageCodec.decode(
                originalVoteNode.toString(), RequestVoteRequestDto.class
        );
        byte[] voteBytes = MessageCodec.encode(voteEnv);
        assertEquals(originalVoteNode, MessageCodec.getMapper().readTree(voteBytes));

        // APPEND_ENTRIES replication round trip
        JsonNode originalAppendNode = fixturesRoot.get("appendEntriesReplicationRequest");
        MessageEnvelope<AppendEntriesRequestDto> appendEnv = MessageCodec.decode(
                originalAppendNode.toString(), AppendEntriesRequestDto.class
        );
        byte[] appendBytes = MessageCodec.encode(appendEnv);
        assertEquals(originalAppendNode, MessageCodec.getMapper().readTree(appendBytes));
    }

    @Test
    @DisplayName("Two-phase decoding: decode raw JsonNode first, inspect type, then map to specific DTO")
    void testTwoPhaseDecoding() throws Exception {
        byte[] rawBytes = fixturesRoot.get("bookTicketRequest").toString().getBytes(StandardCharsets.UTF_8);

        // Phase 1: decode into generic JsonNode envelope
        MessageEnvelope<JsonNode> rawEnvelope = MessageCodec.decode(rawBytes);
        assertEquals(MessageType.BOOK_TICKET, rawEnvelope.getType());

        // Phase 2: map payload based on type
        BookTicketRequestDto payloadDto = MessageCodec.decodePayload(rawEnvelope, BookTicketRequestDto.class);
        assertNotNull(payloadDto);
        payloadDto.validate();
        assertEquals("G05", payloadDto.getSeatId());
        assertEquals("Khach demo", payloadDto.getPassengerName());
    }

    @Test
    @DisplayName("CorrelationId contract: request has null correlationId, response links back to request messageId")
    void testCorrelationIdContract() throws Exception {
        JsonNode reqNode = fixturesRoot.get("bookTicketRequest");
        JsonNode respNode = fixturesRoot.get("bookTicketResponse");

        MessageEnvelope<JsonNode> reqEnv = MessageCodec.decode(reqNode.toString());
        MessageEnvelope<JsonNode> respEnv = MessageCodec.decode(respNode.toString());

        // Request correlationId must be null
        assertNull(reqEnv.getCorrelationId());

        // Response correlationId must match request messageId
        assertEquals(reqEnv.getMessageId(), respEnv.getCorrelationId());
        assertEquals("b4c410df-2b27-45dd-81ac-b7f9c0ec21e4", respEnv.getCorrelationId());
    }

    @Test
    @DisplayName("UTF-8 encoding: complex Vietnamese Unicode passenger name must round-trip cleanly")
    void testVietnameseUnicodeEncoding() throws Exception {
        String vietnameseName = "Nguyễn Trần Khánh Đan - Phòng vé Hà Nội";
        BookTicketRequestDto dto = new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G01", vietnameseName
        );
        MessageEnvelope<BookTicketRequestDto> envelope = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-1", dto
        );
        envelope.validate();

        byte[] bytes = MessageCodec.encode(envelope);
        MessageEnvelope<BookTicketRequestDto> decoded = MessageCodec.decode(bytes, BookTicketRequestDto.class);

        assertEquals(vietnameseName, decoded.getPayload().getPassengerName());
    }

    @Test
    @DisplayName("Negative cases: unknown properties and corrupt JSON must be rejected with exception")
    void testNegativeCases() {
        // 1. Unknown property rejected by FAIL_ON_UNKNOWN_PROPERTIES (SPEC-03 Section 3.0)
        String jsonWithUnknownField = "{\"protocolVersion\":1,\"messageId\":\"b4c410df-2b27-45dd-81ac-b7f9c0ec21e4\",\"correlationId\":null,\"type\":\"BOOK_TICKET\",\"senderId\":\"client-1\",\"sentAt\":\"2026-10-29T10:00:00Z\",\"unknownField\":\"illegal\",\"payload\":{}}";
        assertThrows(IOException.class, () -> MessageCodec.decode(jsonWithUnknownField));

        // 2. Corrupt JSON
        String corruptJson = "{\"protocolVersion\":1, broken...";
        assertThrows(IOException.class, () -> MessageCodec.decode(corruptJson));

        // 3. Null argument checks
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.encode(null));
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode((byte[]) null));
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode((String) null));
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode("  "));
    }

    public static void main(String[] args) throws Exception {
        loadFixtures();
        ProtocolContractTest test = new ProtocolContractTest();

        System.out.println("[TEST 1] testDecodeGoldenFixtures...");
        test.testDecodeGoldenFixtures();
        System.out.println("-> PASS");

        System.out.println("[TEST 2] testRoundTripEquivalence...");
        test.testRoundTripEquivalence();
        System.out.println("-> PASS");

        System.out.println("[TEST 3] testTwoPhaseDecoding...");
        test.testTwoPhaseDecoding();
        System.out.println("-> PASS");

        System.out.println("[TEST 4] testCorrelationIdContract...");
        test.testCorrelationIdContract();
        System.out.println("-> PASS");

        System.out.println("[TEST 5] testVietnameseUnicodeEncoding...");
        test.testVietnameseUnicodeEncoding();
        System.out.println("-> PASS");

        System.out.println("[TEST 6] testNegativeCases...");
        test.testNegativeCases();
        System.out.println("-> PASS");

        System.out.println("\n=== ALL PROTOCOL CONTRACT TESTS COMPLETED: 6/6 PASSED ===");
    }
}
