package vn.nhom6.railway.networking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.Role;

import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for ResponseMapper (SPEC-03 §4.2, §4.4, §7 & TASK TV3-D19)")
public class ResponseMapperTest {

    private final LeaderRouter fixedLeaderRouter = new LeaderRouter(
            "node-1", LeaderRouter.RoleProvider.fixed(Role.LEADER, 5L, "node-1")
    );

    @Test
    @DisplayName("BOOK outcome CONFIRMED: ticketId present, reasonCode null, appliedLogIndex >= 1")
    public void testMapBookOutcomeConfirmed() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        String ticketId = "TKT-CONFIRMED-01";
        ApplyResult result = ApplyResult.bookConfirmed(reqId, ticketId, 42L);

        MessageEnvelope<BookTicketResponseDto> env = ResponseMapper.toBookTicketResponse(
                msgId, "node-1", "SE-001", "G05", result
        );

        assertNotNull(env);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, env.getType());
        assertEquals(msgId, env.getCorrelationId());
        assertEquals("node-1", env.getSenderId());

        BookTicketResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(BookTicketResponseDto.OUTCOME_CONFIRMED, dto.getOutcome());
        assertEquals(reqId, dto.getRequestId());
        assertEquals("SE-001", dto.getTripId());
        assertEquals("G05", dto.getSeatId());
        assertEquals(ticketId, dto.getTicketId());
        assertNull(dto.getReasonCode());
        assertEquals(42L, dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("BOOK outcome REJECTED: SEAT_ALREADY_BOOKED has null ticketId and appliedLogIndex >= 1")
    public void testMapBookOutcomeRejectedSeatAlreadyBooked() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ApplyResult result = ApplyResult.bookRejected(reqId, ApplyResult.SEAT_ALREADY_BOOKED, 43L);

        MessageEnvelope<BookTicketResponseDto> env = ResponseMapper.toBookTicketResponse(
                msgId, "node-1", "SE-001", "G02", result
        );

        assertNotNull(env);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, env.getType());
        assertEquals(msgId, env.getCorrelationId());

        BookTicketResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(BookTicketResponseDto.OUTCOME_REJECTED, dto.getOutcome());
        assertEquals(reqId, dto.getRequestId());
        assertNull(dto.getTicketId());
        assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, dto.getReasonCode());
        assertEquals(43L, dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("BOOK outcome REJECTED: INVALID_REQUEST has null ticketId and appliedLogIndex >= 1")
    public void testMapBookOutcomeRejectedInvalidRequest() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ApplyResult result = ApplyResult.bookRejected(reqId, ApplyResult.INVALID_REQUEST, 44L);

        MessageEnvelope<BookTicketResponseDto> env = ResponseMapper.toBookTicketResponse(
                msgId, "node-1", "SE-001", "G03", result
        );

        assertNotNull(env);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, env.getType());

        BookTicketResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(BookTicketResponseDto.OUTCOME_REJECTED, dto.getOutcome());
        assertEquals(reqId, dto.getRequestId());
        assertNull(dto.getTicketId());
        assertEquals(ApplyResult.INVALID_REQUEST, dto.getReasonCode());
        assertEquals(44L, dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("BOOK outcome UNKNOWN: TIMEOUT_UNKNOWN has null ticketId and null appliedLogIndex")
    public void testMapBookOutcomeUnknown() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ApplyResult result = ApplyResult.bookUnknown(reqId);

        MessageEnvelope<BookTicketResponseDto> env = ResponseMapper.toBookTicketResponse(
                msgId, "node-1", "SE-001", "G04", result
        );

        assertNotNull(env);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, env.getType());

        BookTicketResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(BookTicketResponseDto.OUTCOME_UNKNOWN, dto.getOutcome());
        assertEquals(reqId, dto.getRequestId());
        assertNull(dto.getTicketId());
        assertEquals(ApplyResult.TIMEOUT_UNKNOWN, dto.getReasonCode());
        assertNull(dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("Safety Invariant: Transport/routing errors produce ERROR_RESPONSE, NEVER REJECTED")
    public void testTransportErrorsNeverMappedToRejectedOutcome() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();

        // 1. NO_QUORUM produces ERROR_RESPONSE
        MessageEnvelope<ErrorResponseDto> quorumEnv = ResponseMapper.toNoQuorum(
                msgId, "node-1", reqId, 5L, null
        );
        assertEquals(MessageType.ERROR_RESPONSE, quorumEnv.getType());
        assertEquals("NO_QUORUM", quorumEnv.getPayload().getErrorCode());
        assertNotEquals(MessageType.BOOK_TICKET_RESPONSE, quorumEnv.getType());

        // 2. NOT_LEADER produces ERROR_RESPONSE
        MessageEnvelope<ErrorResponseDto> leaderEnv = ResponseMapper.toNotLeader(
                msgId, "node-2", reqId, "node-1", 5L
        );
        assertEquals(MessageType.ERROR_RESPONSE, leaderEnv.getType());
        assertEquals("NOT_LEADER", leaderEnv.getPayload().getErrorCode());

        // 3. READ_UNAVAILABLE produces ERROR_RESPONSE
        MessageEnvelope<ErrorResponseDto> readEnv = ResponseMapper.toReadUnavailable(
                msgId, "node-1", reqId, 5L, null
        );
        assertEquals(MessageType.ERROR_RESPONSE, readEnv.getType());
        assertEquals("READ_UNAVAILABLE", readEnv.getPayload().getErrorCode());
    }

    @Test
    @DisplayName("GET_REQUEST_RESULT state APPLIED_SUCCESS: non-null commandType, ticketId, appliedLogIndex >= 1")
    public void testGetRequestResultAppliedSuccess() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ResultLookup.LookupResult lookup = ResultLookup.LookupResult.appliedSuccess(
                reqId, "BOOK_TICKET", "TKT-LOOKUP-01", 50L
        );

        MessageEnvelope<GetRequestResultResponseDto> env = ResponseMapper.toGetRequestResultResponse(
                msgId, "node-1", lookup
        );

        assertNotNull(env);
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, env.getType());
        assertEquals(msgId, env.getCorrelationId());

        GetRequestResultResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, dto.getState());
        assertEquals(reqId, dto.getRequestId());
        assertEquals("BOOK_TICKET", dto.getCommandType());
        assertEquals("TKT-LOOKUP-01", dto.getTicketId());
        assertNull(dto.getReasonCode());
        assertEquals(50L, dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("GET_REQUEST_RESULT state APPLIED_REJECTED: non-null reasonCode and appliedLogIndex >= 1")
    public void testGetRequestResultAppliedRejected() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ResultLookup.LookupResult lookup = ResultLookup.LookupResult.appliedRejected(
                reqId, "BOOK_TICKET", "SEAT_ALREADY_BOOKED", 51L
        );

        MessageEnvelope<GetRequestResultResponseDto> env = ResponseMapper.toGetRequestResultResponse(
                msgId, "node-1", lookup
        );

        assertNotNull(env);
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, env.getType());

        GetRequestResultResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_REJECTED, dto.getState());
        assertEquals(reqId, dto.getRequestId());
        assertEquals("BOOK_TICKET", dto.getCommandType());
        assertNull(dto.getTicketId());
        assertEquals("SEAT_ALREADY_BOOKED", dto.getReasonCode());
        assertEquals(51L, dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("GET_REQUEST_RESULT state PENDING: non-null commandType, all other fields null")
    public void testGetRequestResultPending() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ResultLookup.LookupResult lookup = ResultLookup.LookupResult.pending(reqId, "BOOK_TICKET");

        MessageEnvelope<GetRequestResultResponseDto> env = ResponseMapper.toGetRequestResultResponse(
                msgId, "node-1", lookup
        );

        assertNotNull(env);
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, env.getType());

        GetRequestResultResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(GetRequestResultResponseDto.STATE_PENDING, dto.getState());
        assertEquals(reqId, dto.getRequestId());
        assertEquals("BOOK_TICKET", dto.getCommandType());
        assertNull(dto.getTicketId());
        assertNull(dto.getReasonCode());
        assertNull(dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("GET_REQUEST_RESULT state UNKNOWN: all 4 result fields null")
    public void testGetRequestResultUnknown() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        ResultLookup.LookupResult lookup = ResultLookup.LookupResult.unknown(reqId);

        MessageEnvelope<GetRequestResultResponseDto> env = ResponseMapper.toGetRequestResultResponse(
                msgId, "node-1", lookup
        );

        assertNotNull(env);
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, env.getType());

        GetRequestResultResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate();

        assertEquals(GetRequestResultResponseDto.STATE_UNKNOWN, dto.getState());
        assertEquals(reqId, dto.getRequestId());
        assertNull(dto.getCommandType());
        assertNull(dto.getTicketId());
        assertNull(dto.getReasonCode());
        assertNull(dto.getAppliedLogIndex());
    }

    @Test
    @DisplayName("mapThrowable: NotLeaderException maps to NOT_LEADER with hint and term")
    public void testMapThrowableNotLeaderException() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        Exception ex = new BookingRequestHandler.NotLeaderException("node-4");

        MessageEnvelope<ErrorResponseDto> env = ResponseMapper.mapThrowable(
                msgId, "node-2", reqId, ex, fixedLeaderRouter
        );

        assertNotNull(env);
        assertEquals(MessageType.ERROR_RESPONSE, env.getType());
        assertEquals(msgId, env.getCorrelationId());
        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("NOT_LEADER", dto.getErrorCode());
        assertEquals("node-4", dto.getLeaderHint());
        assertEquals(5L, dto.getTerm());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("mapThrowable: NoQuorumException maps to NO_QUORUM with term")
    public void testMapThrowableNoQuorumException() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        Exception ex = new BookingRequestHandler.NoQuorumException();

        MessageEnvelope<ErrorResponseDto> env = ResponseMapper.mapThrowable(
                msgId, "node-1", reqId, ex, fixedLeaderRouter
        );

        assertNotNull(env);
        assertEquals(MessageType.ERROR_RESPONSE, env.getType());
        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("NO_QUORUM", dto.getErrorCode());
        assertEquals(5L, dto.getTerm());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("mapThrowable: ReadUnavailableException maps to READ_UNAVAILABLE")
    public void testMapThrowableReadUnavailableException() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        Exception ex = new BookingRequestHandler.ReadUnavailableException("Read barrier failed");

        MessageEnvelope<ErrorResponseDto> env = ResponseMapper.mapThrowable(
                msgId, "node-1", reqId, ex, fixedLeaderRouter
        );

        assertNotNull(env);
        assertEquals(MessageType.ERROR_RESPONSE, env.getType());
        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("READ_UNAVAILABLE", dto.getErrorCode());
        assertEquals(5L, dto.getTerm());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("mapThrowable: RequestIdConflictException maps to REQUEST_ID_CONFLICT")
    public void testMapThrowableRequestIdConflictException() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        Exception ex = new BookingRequestHandler.RequestIdConflictException();

        MessageEnvelope<ErrorResponseDto> env = ResponseMapper.mapThrowable(
                msgId, "node-1", reqId, ex, fixedLeaderRouter
        );

        assertNotNull(env);
        assertEquals(MessageType.ERROR_RESPONSE, env.getType());
        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("REQUEST_ID_CONFLICT", dto.getErrorCode());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("mapThrowable: IllegalArgumentException maps to INVALID_REQUEST")
    public void testMapThrowableIllegalArgumentException() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        Exception ex = new IllegalArgumentException("Invalid seatId G99");

        MessageEnvelope<ErrorResponseDto> env = ResponseMapper.mapThrowable(
                msgId, "node-1", reqId, ex, fixedLeaderRouter
        );

        assertNotNull(env);
        assertEquals(MessageType.ERROR_RESPONSE, env.getType());
        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("INVALID_REQUEST", dto.getErrorCode());
        assertTrue(dto.getMessage().contains("G99"));
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("mapThrowable: unwraps nested CompletionException and ExecutionException")
    public void testMapThrowableUnwrappedCause() {
        String msgId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();
        Throwable nested = new ExecutionException(new CompletionException(new BookingRequestHandler.NoQuorumException()));

        MessageEnvelope<ErrorResponseDto> env = ResponseMapper.mapThrowable(
                msgId, "node-1", reqId, nested, fixedLeaderRouter
        );

        assertNotNull(env);
        assertEquals("NO_QUORUM", env.getPayload().getErrorCode());
    }
}
