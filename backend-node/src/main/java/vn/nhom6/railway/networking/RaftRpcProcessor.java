package vn.nhom6.railway.networking;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

/**
 * Interface defining the Raft consensus processor boundary for incoming Raft RPCs.
 *
 * <p>Decouples the TCP networking and envelope dispatching layer (TV3) from the
 * underlying Raft consensus engine (TV4/TV5).
 */
public interface RaftRpcProcessor {

    /**
     * Processes an incoming REQUEST_VOTE RPC and produces the vote decision.
     *
     * @param request Validated RequestVote payload
     * @param senderId Remote sender ID
     * @return Vote decision response DTO
     * @throws Exception If an internal failure or storage error occurs
     */
    RequestVoteResponseDto handleRequestVote(RequestVoteRequestDto request, String senderId) throws Exception;

    /**
     * Processes an incoming APPEND_ENTRIES RPC (heartbeat or log entries) and produces the append result.
     *
     * @param request Validated AppendEntries payload
     * @param senderId Remote sender ID
     * @return AppendEntries response DTO
     * @throws Exception If an internal failure or storage error occurs
     */
    AppendEntriesResponseDto handleAppendEntries(AppendEntriesRequestDto request, String senderId) throws Exception;
}
