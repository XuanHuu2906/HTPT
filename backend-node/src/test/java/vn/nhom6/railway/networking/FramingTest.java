package vn.nhom6.railway.networking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class FramingTest {

    @Test
    @DisplayName("Pass criteria: split TCP read must be handled cleanly via readFully")
    void testSplitTcpRead() throws Exception {
        byte[] originalPayload = "TEST-PAYLOAD-FOR-TCP-FRAGMENTATION-SPLIT-STREAM".getBytes(StandardCharsets.UTF_8);
        byte[] framedBytes = FrameCodec.frame(originalPayload);

        // Simulate fragmented TCP chunks where socket only delivers 1 to 3 bytes per read
        InputStream fragmentedIn = new ChunkedInputStream(new ByteArrayInputStream(framedBytes), 2);

        byte[] receivedPayload = FrameCodec.readFrame(fragmentedIn);
        assertArrayEquals(originalPayload, receivedPayload);
    }

    @Test
    @DisplayName("Pass criteria: two concatenated messages on the same socket stream must decode sequentially")
    void testConcatenatedMessages() throws Exception {
        byte[] payload1 = "FIRST_CONCATENATED_MESSAGE".getBytes(StandardCharsets.UTF_8);
        byte[] payload2 = "SECOND_CONCATENATED_MESSAGE".getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeFrame(out, payload1);
        FrameCodec.writeFrame(out, payload2);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        // First message
        byte[] read1 = FrameCodec.readFrame(in);
        assertArrayEquals(payload1, read1);

        // Second message immediately following on the same stream
        byte[] read2 = FrameCodec.readFrame(in);
        assertArrayEquals(payload2, read2);

        // End of stream -> Clean EOF
        assertThrows(EOFException.class, () -> FrameCodec.readFrame(in));
    }

    @Test
    @DisplayName("Pass criteria: payload lower boundary (exactly 1 byte) must frame and unframe cleanly")
    void testPayloadMinBoundary1Byte() throws Exception {
        byte[] singleByte = new byte[]{(byte) 0x7E};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeFrame(out, singleByte);

        byte[] outputBytes = out.toByteArray();
        assertEquals(5, outputBytes.length, "Total frame length must be 4 bytes header + 1 byte data");

        // Verify Big-Endian 4-byte length: 0x00, 0x00, 0x00, 0x01
        assertEquals(0x00, outputBytes[0]);
        assertEquals(0x00, outputBytes[1]);
        assertEquals(0x00, outputBytes[2]);
        assertEquals(0x01, outputBytes[3]);
        assertEquals(0x7E, outputBytes[4]);

        ByteArrayInputStream in = new ByteArrayInputStream(outputBytes);
        byte[] readBytes = FrameCodec.readFrame(in);
        assertArrayEquals(singleByte, readBytes);
    }

    @Test
    @DisplayName("Pass criteria: payload upper boundary (exactly 1 MB = 1,048,576 bytes) must succeed")
    void testPayloadMaxBoundary1MiB() throws Exception {
        int oneMiB = 1_048_576;
        byte[] largePayload = new byte[oneMiB];
        Arrays.fill(largePayload, (byte) 'A');

        ByteArrayOutputStream out = new ByteArrayOutputStream(oneMiB + 4);
        FrameCodec.writeFrame(out, largePayload);

        byte[] framed = out.toByteArray();
        assertEquals(oneMiB + 4, framed.length);

        // Verify Big-Endian 4-byte length for 1,048,576 (0x00100000)
        assertEquals(0x00, framed[0]);
        assertEquals(0x10, framed[1]);
        assertEquals(0x00, framed[2]);
        assertEquals(0x00, framed[3]);

        ByteArrayInputStream in = new ByteArrayInputStream(framed);
        byte[] unFramed = FrameCodec.readFrame(in);
        assertEquals(oneMiB, unFramed.length);
        assertArrayEquals(largePayload, unFramed);
    }

    @Test
    @DisplayName("Oversize payload (> 1 MB) must be rejected on both write and read to prevent OOM")
    void testRejectOversizePayload() {
        int oversize = 1_048_577; // 1 MB + 1 byte
        byte[] oversizePayload = new byte[oversize];

        // 1. Write rejection
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThrows(IllegalArgumentException.class, () -> FrameCodec.writeFrame(out, oversizePayload));

        // 2. Read rejection (simulating malicious sender sending header > 1 MB)
        byte[] fakeOversizeHeader = new byte[]{
                (byte) ((oversize >>> 24) & 0xFF),
                (byte) ((oversize >>> 16) & 0xFF),
                (byte) ((oversize >>> 8) & 0xFF),
                (byte) (oversize & 0xFF),
                0x00 // trailing byte
        };
        ByteArrayInputStream in = new ByteArrayInputStream(fakeOversizeHeader);
        IOException ex = assertThrows(IOException.class, () -> FrameCodec.readFrame(in));
        assertTrue(ex.getMessage().contains("Frame length out of bounds"));
    }

    @Test
    @DisplayName("Payload with zero or negative length must be rejected")
    void testRejectZeroOrNegativeLength() {
        // Zero length write rejection
        assertThrows(IllegalArgumentException.class, () -> FrameCodec.writeFrame(new ByteArrayOutputStream(), new byte[0]));

        // Zero length read rejection
        byte[] zeroHeader = new byte[]{0x00, 0x00, 0x00, 0x00};
        assertThrows(IOException.class, () -> FrameCodec.readFrame(new ByteArrayInputStream(zeroHeader)));

        // Negative length read rejection (e.g. 0xFFFFFFFF = -1)
        byte[] negativeHeader = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThrows(IOException.class, () -> FrameCodec.readFrame(new ByteArrayInputStream(negativeHeader)));
    }

    @Test
    @DisplayName("EOF handling: clean disconnect vs premature truncated frame EOF")
    void testEofHandling() {
        // 1. Clean EOF: stream is empty at frame boundary
        ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[0]);
        EOFException cleanEx = assertThrows(EOFException.class, () -> FrameCodec.readFrame(emptyStream));
        assertTrue(cleanEx.getMessage().contains("clean EOF"));

        // 2. Premature EOF during header (only 2 of 4 bytes available)
        byte[] partialHeader = new byte[]{0x00, 0x00};
        ByteArrayInputStream partialHeaderStream = new ByteArrayInputStream(partialHeader);
        EOFException headerEx = assertThrows(EOFException.class, () -> FrameCodec.readFrame(partialHeaderStream));
        assertTrue(headerEx.getMessage().contains("Premature EOF while reading 4-byte frame length header"));

        // 3. Premature EOF during payload (header says 50 bytes, only 10 available)
        byte[] truncatedFrame = new byte[]{
                0x00, 0x00, 0x00, 0x32, // length = 50
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A // only 10 bytes
        };
        ByteArrayInputStream truncatedStream = new ByteArrayInputStream(truncatedFrame);
        EOFException payloadEx = assertThrows(EOFException.class, () -> FrameCodec.readFrame(truncatedStream));
        assertTrue(payloadEx.getMessage().contains("Premature EOF while reading frame payload"));
    }

    @Test
    @DisplayName("End-to-end integration: write and read MessageEnvelope with typed DTO over framed stream")
    void testMessageEnvelopeFramingRoundTrip() throws Exception {
        BookTicketRequestDto bookDto = new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G05", "Nguyễn Văn Demo"
        );
        MessageEnvelope<BookTicketRequestDto> originalEnvelope = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-1", bookDto
        );
        originalEnvelope.validate();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeMessage(out, originalEnvelope);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        MessageEnvelope<BookTicketRequestDto> receivedEnvelope = FrameCodec.readMessage(in, BookTicketRequestDto.class);

        assertEquals(originalEnvelope.getMessageId(), receivedEnvelope.getMessageId());
        assertEquals(originalEnvelope.getType(), receivedEnvelope.getType());
        assertEquals("SE-001", receivedEnvelope.getPayload().getTripId());
        assertEquals("G05", receivedEnvelope.getPayload().getSeatId());
        assertEquals("Nguyễn Văn Demo", receivedEnvelope.getPayload().getPassengerName());
    }

    public static void main(String[] args) throws Exception {
        FramingTest test = new FramingTest();

        System.out.println("[TEST 1] testSplitTcpRead...");
        test.testSplitTcpRead();
        System.out.println("-> PASS");

        System.out.println("[TEST 2] testConcatenatedMessages...");
        test.testConcatenatedMessages();
        System.out.println("-> PASS");

        System.out.println("[TEST 3] testPayloadMinBoundary1Byte...");
        test.testPayloadMinBoundary1Byte();
        System.out.println("-> PASS");

        System.out.println("[TEST 4] testPayloadMaxBoundary1MiB...");
        test.testPayloadMaxBoundary1MiB();
        System.out.println("-> PASS");

        System.out.println("[TEST 5] testRejectOversizePayload...");
        test.testRejectOversizePayload();
        System.out.println("-> PASS");

        System.out.println("[TEST 6] testRejectZeroOrNegativeLength...");
        test.testRejectZeroOrNegativeLength();
        System.out.println("-> PASS");

        System.out.println("[TEST 7] testEofHandling...");
        test.testEofHandling();
        System.out.println("-> PASS");

        System.out.println("[TEST 8] testMessageEnvelopeFramingRoundTrip...");
        test.testMessageEnvelopeFramingRoundTrip();
        System.out.println("-> PASS");

        System.out.println("\n=== ALL FRAMING TESTS COMPLETED: 8/8 PASSED ===");
    }

    /**
     * Helper stream to simulate fragmented TCP network reads.
     */
    private static class ChunkedInputStream extends FilterInputStream {
        private final int maxChunkSize;

        ChunkedInputStream(InputStream in, int maxChunkSize) {
            super(in);
            this.maxChunkSize = maxChunkSize;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int toRead = Math.min(len, maxChunkSize);
            return super.read(b, off, toRead);
        }
    }
}
