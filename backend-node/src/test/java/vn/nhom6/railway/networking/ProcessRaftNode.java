package vn.nhom6.railway.networking;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;

/**
 * Standalone Raft peer node runner for multi-process TCP integration tests.
 *
 * <p>Spun up via {@link ProcessBuilder} to prove inter-process communication
 * exchanging REQUEST_VOTE and APPEND_ENTRIES over TCP.
 */
public class ProcessRaftNode {

    public static void main(String[] args) throws IOException, InterruptedException {
        int requestedPort = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        String nodeId = args.length > 1 ? args[1] : "child-node";

        HandlerRegistry registry = new HandlerRegistry();

        registry.register(MessageType.REQUEST_VOTE, req -> {
            RequestVoteRequestDto voteReq = MessageCodec.decodePayload(req, RequestVoteRequestDto.class);
            System.out.println("[ChildProcess] Handled REQUEST_VOTE from=" + voteReq.getCandidateId() + " term=" + voteReq.getTerm());
            System.out.flush();

            RequestVoteResponseDto resp = RequestVoteResponseDto.granted(voteReq.getTerm());
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, nodeId, resp);
        });

        registry.register(MessageType.APPEND_ENTRIES, req -> {
            AppendEntriesRequestDto appendReq = MessageCodec.decodePayload(req, AppendEntriesRequestDto.class);
            long matchIndex = appendReq.getPrevLogIndex() + appendReq.getEntries().size();
            System.out.println("[ChildProcess] Handled APPEND_ENTRIES leader=" + appendReq.getLeaderId() + " term=" + appendReq.getTerm() + " matchIndex=" + matchIndex);
            System.out.flush();

            AppendEntriesResponseDto resp = AppendEntriesResponseDto.success(appendReq.getTerm(), matchIndex);
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.APPEND_ENTRIES_RESPONSE, nodeId, resp);
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, nodeId);
        TcpServer server = new TcpServer("127.0.0.1", requestedPort).setMessageHandler(dispatcher::dispatch);
        server.start();

        int boundPort = server.getPort();
        System.out.println("READY:" + boundPort);
        System.out.flush();

        // Keep process running until JVM is terminated
        Thread.sleep(60_000);
    }
}
