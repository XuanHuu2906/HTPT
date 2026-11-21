package vn.nhom6.railway.networking;

import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.logging.Logger;

/**
 * In-memory TCP Network Fault and Partition Filter for integration testing.
 *
 * <p>Enables realistic simulation of distributed network anomalies (split-brain,
 * majority/minority partition, node isolation, asymmetric packet loss, message drop by type)
 * across real multi-node TCP clusters without terminating JVM processes or unbinding sockets
 * (in strict compliance with AGENTS.md §0, §5 and SPEC-07 TC-RAFT-06).
 */
public class TestNetworkFilter implements MessageDispatcher.MessageFilterHook, RaftRpcClient.RpcFilterHook {

    private static final Logger LOGGER = Logger.getLogger(TestNetworkFilter.class.getName());

    /**
     * Immutable directed link representation.
     */
    public record DirectedLink(String from, String to) {
        public DirectedLink {
            Objects.requireNonNull(from, "from must not be null");
            Objects.requireNonNull(to, "to must not be null");
        }

        public DirectedLink reverse() {
            return new DirectedLink(to, from);
        }
    }

    // Set of directed links currently blocked: (from -> to)
    private final Set<DirectedLink> blockedLinks = ConcurrentHashMap.newKeySet();

    // Set of fully isolated node IDs (all inbound and outbound traffic dropped)
    private final Set<String> isolatedNodes = ConcurrentHashMap.newKeySet();

    // Custom predicates for selective message dropping (e.g. drop only REQUEST_VOTE)
    private final List<BiPredicate<DirectedLink, MessageEnvelope<?>>> customDropPredicates = new CopyOnWriteArrayList<>();

    // Telemetry and Metrics
    private final AtomicLong totalDroppedCount = new AtomicLong(0);
    private final AtomicLong totalDeliveredCount = new AtomicLong(0);
    private final ConcurrentMap<DirectedLink, AtomicLong> droppedByLink = new ConcurrentHashMap<>();

    public TestNetworkFilter() {}

    // =========================================================================
    // Core Hook Implementation
    // =========================================================================

    @Override
    public boolean shouldDeliver(String from, String to, MessageEnvelope<?> message) {
        if (from == null || to == null) {
            // If sender or recipient is anonymous, allow delivery by default
            totalDeliveredCount.incrementAndGet();
            return true;
        }

        DirectedLink link = new DirectedLink(from, to);

        // 1. Check if either endpoint is completely isolated
        if (isolatedNodes.contains(from) || isolatedNodes.contains(to)) {
            recordDrop(link, message, "Isolated node (" + (isolatedNodes.contains(from) ? from : to) + ")");
            return false;
        }

        // 2. Check if this specific directed link is blocked
        if (blockedLinks.contains(link)) {
            recordDrop(link, message, "Blocked link (" + from + " -> " + to + ")");
            return false;
        }

        // 3. Evaluate any custom drop predicates (e.g. selective message type drops)
        for (BiPredicate<DirectedLink, MessageEnvelope<?>> predicate : customDropPredicates) {
            if (predicate.test(link, message)) {
                recordDrop(link, message, "Custom drop predicate matched");
                return false;
            }
        }

        // Message allowed
        totalDeliveredCount.incrementAndGet();
        return true;
    }

    private void recordDrop(DirectedLink link, MessageEnvelope<?> message, String reason) {
        totalDroppedCount.incrementAndGet();
        droppedByLink.computeIfAbsent(link, k -> new AtomicLong(0)).incrementAndGet();
        LOGGER.fine(() -> "[TestNetworkFilter] DROPPED: " + link.from() + " -> " + link.to()
                + " [type=" + (message != null ? message.getType() : "null")
                + ", msgId=" + (message != null ? message.getMessageId() : "null")
                + "] Reason: " + reason);
    }

    // =========================================================================
    // Partitioning & Topology Control
    // =========================================================================

    /**
     * Splits nodes into two mutually isolated network partitions (A and B).
     * Traffic within A is allowed, traffic within B is allowed.
     * Traffic between A and B is blocked bidirectionally.
     *
     * @param partitionA Nodes in partition A
     * @param partitionB Nodes in partition B
     */
    public void partition(Set<String> partitionA, Set<String> partitionB) {
        Objects.requireNonNull(partitionA, "partitionA must not be null");
        Objects.requireNonNull(partitionB, "partitionB must not be null");

        for (String nodeA : partitionA) {
            for (String nodeB : partitionB) {
                blockBiLink(nodeA, nodeB);
            }
        }
        LOGGER.info(() -> "[TestNetworkFilter] Partition created: A=" + partitionA + " <X> B=" + partitionB);
    }

    /**
     * Splits nodes into N mutually isolated network partitions.
     * Nodes within the same partition can communicate.
     * Nodes in different partitions cannot communicate.
     *
     * @param partitions List of node sets representing isolated partitions
     */
    public void partition(List<Set<String>> partitions) {
        Objects.requireNonNull(partitions, "partitions must not be null");
        for (int i = 0; i < partitions.size(); i++) {
            for (int j = i + 1; j < partitions.size(); j++) {
                partition(partitions.get(i), partitions.get(j));
            }
        }
    }

    /**
     * Completely isolates a node from the rest of the cluster.
     * All incoming and outgoing traffic for this node will be dropped.
     *
     * @param nodeId Node to isolate
     */
    public void isolate(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        isolatedNodes.add(nodeId);
        LOGGER.info(() -> "[TestNetworkFilter] Node isolated: " + nodeId);
    }

    /**
     * De-isolates a node.
     *
     * @param nodeId Node to de-isolate
     */
    public void deisolate(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        isolatedNodes.remove(nodeId);
        LOGGER.info(() -> "[TestNetworkFilter] Node de-isolated: " + nodeId);
    }

    /**
     * Blocks a unidirectional link from {@code fromNode} to {@code toNode}.
     * The reverse direction (to -> from) remains unaffected.
     */
    public void blockLink(String fromNode, String toNode) {
        blockedLinks.add(new DirectedLink(fromNode, toNode));
        LOGGER.info(() -> "[TestNetworkFilter] Unidirectional link blocked: " + fromNode + " -> " + toNode);
    }

    /**
     * Blocks a bidirectional communication link between {@code nodeA} and {@code nodeB}.
     */
    public void blockBiLink(String nodeA, String nodeB) {
        blockLink(nodeA, nodeB);
        blockLink(nodeB, nodeA);
    }

    /**
     * Restores a unidirectional link from {@code fromNode} to {@code toNode}.
     */
    public void allowLink(String fromNode, String toNode) {
        blockedLinks.remove(new DirectedLink(fromNode, toNode));
        LOGGER.info(() -> "[TestNetworkFilter] Link restored: " + fromNode + " -> " + toNode);
    }

    /**
     * Restores bidirectional communication between {@code nodeA} and {@code nodeB}.
     */
    public void allowBiLink(String nodeA, String nodeB) {
        allowLink(nodeA, nodeB);
        allowLink(nodeB, nodeA);
    }

    /**
     * Heals the entire network: clears all partition boundaries, isolated nodes,
     * blocked links, and custom drop rules. Restores full mesh connectivity.
     */
    public void heal() {
        blockedLinks.clear();
        isolatedNodes.clear();
        customDropPredicates.clear();
        LOGGER.info("[TestNetworkFilter] Network fully HEALED: all drop rules and partitions cleared");
    }

    // =========================================================================
    // Selective Message Dropping (By Type / Predicate)
    // =========================================================================

    /**
     * Drops all messages of a specific type across the entire cluster.
     */
    public void dropMessageType(MessageType type) {
        Objects.requireNonNull(type, "type must not be null");
        customDropPredicates.add((link, msg) -> msg != null && msg.getType() == type);
        LOGGER.info(() -> "[TestNetworkFilter] Added rule: drop all messages of type " + type);
    }

    /**
     * Drops messages of a specific type on a specific directed link.
     */
    public void dropMessageTypeOnLink(String fromNode, String toNode, MessageType type) {
        Objects.requireNonNull(fromNode, "fromNode must not be null");
        Objects.requireNonNull(toNode, "toNode must not be null");
        Objects.requireNonNull(type, "type must not be null");

        DirectedLink targetLink = new DirectedLink(fromNode, toNode);
        customDropPredicates.add((link, msg) -> link.equals(targetLink) && msg != null && msg.getType() == type);
        LOGGER.info(() -> "[TestNetworkFilter] Added rule: drop " + type + " on link " + fromNode + " -> " + toNode);
    }

    /**
     * Adds a generic custom drop predicate.
     */
    public void addDropPredicate(BiPredicate<DirectedLink, MessageEnvelope<?>> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");
        customDropPredicates.add(predicate);
    }

    // =========================================================================
    // Cluster Wiring Utilities
    // =========================================================================

    /**
     * Attaches this filter to both peer (Raft) and client dispatchers of a {@link NodeMain}.
     */
    public void attachTo(NodeMain node) {
        Objects.requireNonNull(node, "node must not be null");
        if (node.getRaftDispatcher() != null) {
            node.getRaftDispatcher().setFilterHook(this);
        }
        if (node.getClientDispatcher() != null) {
            node.getClientDispatcher().setFilterHook(this);
        }
    }

    /**
     * Attaches this filter to all nodes in a cluster map.
     */
    public void attachToCluster(Map<String, NodeMain> cluster) {
        Objects.requireNonNull(cluster, "cluster must not be null");
        cluster.values().forEach(this::attachTo);
        LOGGER.info(() -> "[TestNetworkFilter] Attached to " + cluster.size() + " cluster nodes");
    }

    /**
     * Detaches this filter from all nodes in a cluster map.
     */
    public void detachFromCluster(Map<String, NodeMain> cluster) {
        if (cluster == null) return;
        for (NodeMain node : cluster.values()) {
            if (node.getRaftDispatcher() != null) {
                node.getRaftDispatcher().setFilterHook(null);
            }
            if (node.getClientDispatcher() != null) {
                node.getClientDispatcher().setFilterHook(null);
            }
        }
        LOGGER.info("[TestNetworkFilter] Detached from cluster nodes");
    }

    /**
     * Attaches this filter to all {@link RaftRpcClient} instances in a map.
     */
    public void attachToClients(Map<String, RaftRpcClient> clients) {
        Objects.requireNonNull(clients, "clients must not be null");
        clients.values().forEach(client -> client.setFilterHook(this));
        LOGGER.info(() -> "[TestNetworkFilter] Attached to " + clients.size() + " RaftRpcClients");
    }

    /**
     * Detaches this filter from all {@link RaftRpcClient} instances in a map.
     */
    public void detachFromClients(Map<String, RaftRpcClient> clients) {
        if (clients == null) return;
        clients.values().forEach(client -> client.setFilterHook(null));
        LOGGER.info("[TestNetworkFilter] Detached from RaftRpcClients");
    }

    // =========================================================================
    // Metrics & Inspection
    // =========================================================================

    public long getDroppedCount() {
        return totalDroppedCount.get();
    }

    public long getDeliveredCount() {
        return totalDeliveredCount.get();
    }

    public long getDroppedCount(String from, String to) {
        AtomicLong count = droppedByLink.get(new DirectedLink(from, to));
        return count != null ? count.get() : 0L;
    }

    public Set<DirectedLink> getBlockedLinks() {
        return Collections.unmodifiableSet(blockedLinks);
    }

    public Set<String> getIsolatedNodes() {
        return Collections.unmodifiableSet(isolatedNodes);
    }

    public void resetMetrics() {
        totalDroppedCount.set(0);
        totalDeliveredCount.set(0);
        droppedByLink.clear();
    }
}
