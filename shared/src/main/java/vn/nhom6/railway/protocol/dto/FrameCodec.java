package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Length-prefixed TCP framing codec according to SPEC-03 Section 2.
 *
 * <p>Framing structure:
 * <ul>
 *   <li>4 bytes unsigned length in network byte order (Big-Endian)</li>
 *   <li>N bytes UTF-8 JSON payload (1 &lt;= N &lt;= 1,048,576 bytes / 1 MiB)</li>
 * </ul>
 *
 * <p>Guarantees safe {@code readFully} behavior to handle TCP stream fragmentation
 * and multiple concatenated messages on the same socket stream.</p>
 */
public final class FrameCodec {

    public static final int MIN_FRAME_LENGTH = 1;
    public static final int MAX_FRAME_LENGTH = 1_048_576; // 1 MiB (SPEC-03 Section 2)

    private FrameCodec() {
        // Utility class
    }

    /**
     * Writes a 4-byte big-endian length prefix followed by the payload bytes to the stream.
     *
     * @param out Target output stream (e.g. Socket.getOutputStream())
     * @param payload Raw payload bytes (must be between 1 and 1,048,576 bytes)
     * @throws IOException If write fails or payload length violates bounds
     */
    public static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("OutputStream must not be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        int len = payload.length;
        if (len < MIN_FRAME_LENGTH || len > MAX_FRAME_LENGTH) {
            throw new IllegalArgumentException("Payload length out of bounds: " + len + ", allowed: [" + MIN_FRAME_LENGTH + ", " + MAX_FRAME_LENGTH + "]");
        }

        // 4 bytes unsigned length, network byte order (big-endian)
        out.write((len >>> 24) & 0xFF);
        out.write((len >>> 16) & 0xFF);
        out.write((len >>> 8) & 0xFF);
        out.write(len & 0xFF);

        out.write(payload);
        out.flush();
    }

    /**
     * Packages a payload byte array into a complete framed byte array: [4-byte BE length || payload].
     *
     * @param payload Raw payload bytes
     * @return Framed byte array
     */
    public static byte[] frame(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        int len = payload.length;
        if (len < MIN_FRAME_LENGTH || len > MAX_FRAME_LENGTH) {
            throw new IllegalArgumentException("Payload length out of bounds: " + len + ", allowed: [" + MIN_FRAME_LENGTH + ", " + MAX_FRAME_LENGTH + "]");
        }
        byte[] framed = new byte[4 + len];
        framed[0] = (byte) ((len >>> 24) & 0xFF);
        framed[1] = (byte) ((len >>> 16) & 0xFF);
        framed[2] = (byte) ((len >>> 8) & 0xFF);
        framed[3] = (byte) (len & 0xFF);
        System.arraycopy(payload, 0, framed, 4, len);
        return framed;
    }

    /**
     * Encodes a {@link MessageEnvelope} into JSON and writes it with 4-byte length prefix.
     *
     * @param out Target output stream
     * @param envelope Message envelope to serialize and frame
     * @throws IOException If encoding or writing fails
     */
    public static void writeMessage(OutputStream out, MessageEnvelope<?> envelope) throws IOException {
        byte[] bytes = MessageCodec.encode(envelope);
        writeFrame(out, bytes);
    }

    /**
     * Reads a single length-prefixed frame from the input stream.
     *
     * @param in Input stream (e.g. Socket.getInputStream())
     * @return Exactly N bytes payload
     * @throws EOFException If EOF is reached cleanly at frame boundary, or premature EOF mid-frame
     * @throws IOException If length is out of bounds or stream read error occurs
     */
    public static byte[] readFrame(InputStream in) throws IOException {
        if (in == null) {
            throw new IllegalArgumentException("InputStream must not be null");
        }

        byte[] header = new byte[4];
        int firstByte = in.read();
        if (firstByte == -1) {
            throw new EOFException("Connection closed by peer (clean EOF)");
        }
        header[0] = (byte) firstByte;
        readFully(in, header, 1, 3, "Premature EOF while reading 4-byte frame length header");

        int len = ((header[0] & 0xFF) << 24)
                | ((header[1] & 0xFF) << 16)
                | ((header[2] & 0xFF) << 8)
                | (header[3] & 0xFF);

        if (len < MIN_FRAME_LENGTH || len > MAX_FRAME_LENGTH) {
            throw new IOException("Frame length out of bounds: " + len + ", allowed: [" + MIN_FRAME_LENGTH + ", " + MAX_FRAME_LENGTH + "]");
        }

        byte[] payload = new byte[len];
        readFully(in, payload, 0, len, "Premature EOF while reading frame payload");
        return payload;
    }

    /**
     * Reads a length-prefixed frame and decodes it into a {@link MessageEnvelope} with {@link JsonNode} payload.
     *
     * @param in Target input stream
     * @return Decoded envelope
     * @throws IOException If read or JSON parse fails
     */
    public static MessageEnvelope<JsonNode> readMessage(InputStream in) throws IOException {
        byte[] payload = readFrame(in);
        return MessageCodec.decode(payload);
    }

    /**
     * Reads a length-prefixed frame and decodes it directly into a typed {@link MessageEnvelope}.
     *
     * @param in Target input stream
     * @param payloadClass Class of the payload DTO
     * @param <T> Payload type
     * @return Decoded strongly-typed envelope
     * @throws IOException If read or JSON parse fails
     */
    public static <T> MessageEnvelope<T> readMessage(InputStream in, Class<T> payloadClass) throws IOException {
        byte[] payload = readFrame(in);
        return MessageCodec.decode(payload, payloadClass);
    }

    /**
     * Reads exactly {@code length} bytes into the given buffer starting from {@code offset}.
     * Loop-reads until all requested bytes are consumed, handling TCP chunk fragmentation.
     *
     * @param in Stream to read from
     * @param buffer Destination buffer
     * @param offset Starting offset in buffer
     * @param length Number of bytes required
     * @param eofMessage Error description if EOF is encountered prematurely
     * @throws EOFException If stream closes before reading {@code length} bytes
     * @throws IOException On read failure
     */
    public static void readFully(InputStream in, byte[] buffer, int offset, int length, String eofMessage) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(buffer, offset + totalRead, length - totalRead);
            if (read == -1) {
                throw new EOFException(eofMessage + ": expected " + length + " bytes, read " + totalRead);
            }
            totalRead += read;
        }
    }
}
