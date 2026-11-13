package vn.nhom6.railway.networking;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.persistence.DbStateLoader;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.election.*;

/** Composition for real TCP election/heartbeat, fixed five voters and per-node PostgreSQL. */
public final class ElectionRuntime implements AutoCloseable {
    private final RaftRpcClient rpc;
    private final RaftElectionComponent election;
    private final java.util.concurrent.ScheduledThreadPoolExecutor deadlines =
            new java.util.concurrent.ScheduledThreadPoolExecutor(1, task -> {
                Thread thread = new Thread(task, "raft-rpc-deadlines");
                thread.setDaemon(true);
                return thread;
            });

    public ElectionRuntime(NodeConfig config, Map<String, String> env, Consumer<Exception> failure)
            throws Exception {
        var endpoints = parsePeers(env.get("NODE_PEERS"));
        var factory = new ConnectionFactory(java.util.Objects.requireNonNull(config.dbConfig(),
                "Election runtime requires PostgreSQL"));
        var storage = new JdbcRaftStorage(factory);
        // Validate D13 before constructing core, timers or any TCP listener.
        new DbStateLoader(storage, new JdbcBookingRepository(factory), factory).load();
        Duration deadline = Duration.ofMillis(number(env, "TCP_RESPONSE_DEADLINE_MS", 1000));
        var timers = new ElectionConfig(number(env, "RAFT_HEARTBEAT_INTERVAL_MS", 150),
                number(env, "RAFT_ELECTION_TIMEOUT_MIN_MS", 500),
                number(env, "RAFT_ELECTION_TIMEOUT_MAX_MS", 1000));
        rpc = new RaftRpcClient(config.nodeId(), endpoints, 8,
                Math.toIntExact(number(env, "TCP_CONNECT_TIMEOUT_MS", 300)), deadline.multipliedBy(2));
        var reference = new AtomicReference<RaftElectionComponent>();
        java.util.concurrent.Executor loop = task -> reference.get().execute(task);
        try {
            var votes = new VoteRpcAdapter(config.nodeId(), rpc::exchange, deadline, loop, failure);
            election = new RaftElectionComponent(config.nodeId(), storage,
                    (state, durable) -> new PersistentRaftNode(config.nodeId(), state, durable),
                    votes, timers, failure, () -> {});
            reference.set(election);
            deadlines.setRemoveOnCancelPolicy(true);
            var appendTransport = new vn.nhom6.railway.raft.core.RaftTransportAdapter(
                    config.nodeId(), rpc::exchange, deadline, loop, deadlines, failure);
            var busy = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
            election.configureHeartbeat((peer, request, reply) -> {
                if (!busy.add(peer)) return; // One unresolved heartbeat per peer.
                try {
                    appendTransport.sendAppendEntries(peer, new RaftNode.AppendEntriesRequest(
                            request.getTerm(), request.getLeaderId(), request.getPrevLogIndex(),
                            request.getPrevLogTerm(), java.util.List.of(), request.getLeaderCommit()), result -> {
                        if (!(result instanceof vn.nhom6.railway.raft.core.RaftTransportAdapter.LateHigherTerm)) {
                            busy.remove(peer);
                        }
                        if (result instanceof vn.nhom6.railway.raft.core.RaftTransportAdapter.Replied response) {
                            reply.accept(response.response());
                        } else if (result instanceof vn.nhom6.railway.raft.core.RaftTransportAdapter.LateHigherTerm late) {
                            reply.accept(new RaftNode.AppendEntriesResponse(late.term(), false, 0));
                        }
                    });
                } catch (RuntimeException localFailure) {
                    busy.remove(peer);
                    throw localFailure;
                }
            });
        } catch (Exception startup) {
            rpc.close();
            deadlines.shutdownNow();
            if (reference.get() != null) reference.get().close();
            throw startup;
        }
    }

    public RaftElectionComponent election() { return election; }

    public static Map<String, InetSocketAddress> parsePeers(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("NODE_PEERS is required");
        Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
        for (String token : value.split(",", -1)) {
            String[] pair = token.trim().split("@", -1);
            if (pair.length != 2 || !pair[0].matches("node-[1-5]")) {
                throw new IllegalArgumentException("NODE_PEERS expects node-N@host:port");
            }
            int colon = pair[1].lastIndexOf(':');
            if (colon <= 0) throw new IllegalArgumentException("peer host:port required");
            int port = Integer.parseInt(pair[1].substring(colon + 1));
            if (port < 1 || port > 65535) throw new IllegalArgumentException("peer port out of range");
            if (peers.put(pair[0], InetSocketAddress.createUnresolved(pair[1].substring(0, colon), port)) != null) {
                throw new IllegalArgumentException("duplicate peer id");
            }
        }
        if (!peers.keySet().equals(Set.of("node-1", "node-2", "node-3", "node-4", "node-5"))) {
            throw new IllegalArgumentException("exactly five configured voters required");
        }
        return Map.copyOf(peers);
    }

    private static long number(Map<String, String> env, String key, long fallback) {
        return env.containsKey(key) ? Long.parseLong(env.get(key)) : fallback;
    }

    @Override public void close() {
        election.close();
        rpc.close();
        deadlines.shutdownNow();
    }
}
